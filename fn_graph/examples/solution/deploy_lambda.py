"""
Deploy fn_graph Lambda worker.

Two modes:
  --mode zip    (default) Package handler + cloudpickle into a .zip, upload directly.
                No ECR, no Docker needed. Use this for testing.
  --mode ecr    Build Docker image, push to ECR, deploy container Lambda.
                Use this for production (handles sklearn/numpy size limit).

Usage:
    # Zip deploy (testing — no ECR needed)
    python deploy_lambda.py --mode zip --region us-east-1 --role arn:aws:iam::123456789012:role/my-role

    # ECR deploy (production)
    python deploy_lambda.py --mode ecr --region us-east-1 --account 123456789012 --role arn:aws:iam::123456789012:role/my-role
"""

import io
import subprocess
import sys
import zipfile
from pathlib import Path


def run(cmd: list, check=True):
    print(f"[deploy] $ {' '.join(cmd)}", flush=True)
    return subprocess.run(cmd, capture_output=False, text=True, check=check)


def deploy_zip(args):
    import tempfile, shutil
    print(f"\n[deploy/zip] function: {args.function}")
    print(f"[deploy/zip] region:   {args.region}")

    print("\n[deploy/zip] === step 1: building zip ===")
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        handler_path = Path(__file__).parent / "worker" / "lambda_handler.py"
        print(f"[deploy/zip] adding {handler_path}", flush=True)
        zf.write(handler_path, "lambda_handler.py")

        print(f"[deploy/zip] installing cloudpickle...", flush=True)
        tmp = Path(tempfile.mkdtemp())
        subprocess.run(
            [sys.executable, "-m", "pip", "install", "cloudpickle", "-t", str(tmp), "-q"],
            check=True,
        )
        for file in tmp.rglob("*"):
            if file.is_file() and "__pycache__" not in str(file):
                zf.write(file, str(file.relative_to(tmp)))
        shutil.rmtree(tmp)

    zip_bytes = zip_buffer.getvalue()
    print(f"[deploy/zip] zip size: {len(zip_bytes) / (1024*1024):.2f} MB", flush=True)

    print("\n[deploy/zip] === step 2: create or update Lambda function ===")
    import boto3
    client = boto3.client("lambda", region_name=args.region)

    try:
        client.get_function(FunctionName=args.function)
        exists = True
    except client.exceptions.ResourceNotFoundException:
        exists = False

    if exists:
        print(f"[deploy/zip] updating existing function: {args.function}", flush=True)
        client.update_function_code(FunctionName=args.function, ZipFile=zip_bytes)
    else:
        if not args.role:
            print("[deploy/zip] ERROR: --role is required when creating a new Lambda function.")
            sys.exit(1)
        print(f"[deploy/zip] creating new function: {args.function}", flush=True)
        client.create_function(
            FunctionName=args.function,
            Runtime="python3.12",
            Role=args.role,
            Handler="lambda_handler.handler",
            Code={"ZipFile": zip_bytes},
            Timeout=300,
            MemorySize=512,
        )

    print(f"\n[deploy/zip] done. '{args.function}' is live in {args.region}")
    print(f"[deploy/zip] test with: python toy_pipeline.py --config config/toy_lambda.yaml")


def deploy_ecr(args):
    if not args.account:
        print("[deploy/ecr] ERROR: --account is required for ECR deploy.")
        sys.exit(1)

    image_name   = args.function
    ecr_registry = f"{args.account}.dkr.ecr.{args.region}.amazonaws.com"
    ecr_uri      = f"{ecr_registry}/{image_name}:latest"

    print(f"\n[deploy/ecr] region:   {args.region}")
    print(f"[deploy/ecr] account:  {args.account}")
    print(f"[deploy/ecr] ECR URI:  {ecr_uri}")

    print("\n[deploy/ecr] === step 1: ECR login ===")
    login = subprocess.run(
        ["aws", "ecr", "get-login-password", "--region", args.region],
        capture_output=True, text=True, check=True,
    )
    subprocess.run(
        ["docker", "login", "--username", "AWS", "--password-stdin", ecr_registry],
        input=login.stdout, text=True, check=True,
    )

    print("\n[deploy/ecr] === step 2: ensure ECR repo exists ===")
    subprocess.run(
        ["aws", "ecr", "create-repository", "--repository-name", image_name, "--region", args.region],
        capture_output=True, check=False,
    )

    print("\n[deploy/ecr] === step 3: build image ===")
    run(["docker", "build", "-f", "worker/Dockerfile.lambda", "-t", image_name, "worker/"])

    print("\n[deploy/ecr] === step 4: tag and push ===")
    run(["docker", "tag", f"{image_name}:latest", ecr_uri])
    run(["docker", "push", ecr_uri])

    print("\n[deploy/ecr] === step 5: create or update Lambda function ===")
    check = subprocess.run(
        ["aws", "lambda", "get-function", "--function-name", args.function, "--region", args.region],
        capture_output=True, check=False,
    )

    if check.returncode == 0:
        run(["aws", "lambda", "update-function-code", "--function-name", args.function,
             "--image-uri", ecr_uri, "--region", args.region])
    else:
        if not args.role:
            print("[deploy/ecr] ERROR: --role is required when creating a new Lambda function.")
            sys.exit(1)
        run(["aws", "lambda", "create-function", "--function-name", args.function,
             "--package-type", "Image", "--code", f"ImageUri={ecr_uri}",
             "--role", args.role, "--region", args.region, "--timeout", "300", "--memory-size", "512"])

    print(f"\n[deploy/ecr] done. '{args.function}' is live at {ecr_uri}")
    print(f"[deploy/ecr] test with: python toy_pipeline.py --config config/toy_lambda.yaml")


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Deploy fn_graph Lambda worker.")
    parser.add_argument("--mode",     choices=["zip", "ecr"], default="zip",
                        help="zip = direct upload, no ECR (default). ecr = Docker image via ECR.")
    parser.add_argument("--region",   required=True,  help="AWS region, e.g. us-east-1")
    parser.add_argument("--account",  default="",     help="AWS account ID (required for ecr mode)")
    parser.add_argument("--function", default="fn_graph_worker_lambda", help="Lambda function name")
    parser.add_argument("--role",     default="",     help="IAM role ARN (required on first deploy)")
    args = parser.parse_args()

    if args.mode == "zip":
        deploy_zip(args)
    else:
        deploy_ecr(args)


if __name__ == "__main__":
    main()

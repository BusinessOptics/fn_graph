from fn_graph import Composer


def function_0():
    return 42


def function_1():
    return 43


def function_2(function_1):
    return function_1


def function_3(function_1):
    return function_1


def function_4(function_0):
    return function_0


def function_5(function_2):
    return function_2


def function_6(function_0, function_2, function_3, function_5):
    return function_3 + function_2 + function_5 + function_0


def function_7(function_2, function_3, function_5, function_6):
    return function_3 + function_2 + function_6 + function_5


# making these links

# def function_8(function_7):
#     return function_7


# def function_9(function_5):
#     return function_5


def function_10(function_1, function_3, function_6, function_7, function_9):
    return function_3 + function_1 + function_6 + function_7 + function_9


def function_11(function_1, function_4):
    return function_4 + function_1


def function_12(function_3, function_5, function_6, function_8, function_9):
    return function_3 + function_6 + function_8 + function_9 + function_5


def function_13(function_0, function_12, function_4, function_8):
    return function_12 + function_4 + function_8 + function_0


def function_14(function_7, function_9):
    return function_7 + function_9


def function_15(function_0, function_14, function_3, function_7):
    return function_3 + function_7 + function_14 + function_0


def function_16(function_0, function_1, function_11, function_3, function_6):
    return function_11 + function_3 + function_1 + function_6 + function_0


def function_17(function_2, function_8):
    return function_2 + function_8


def function_18(function_10, function_13, function_14, function_4):
    return function_4 + function_10 + function_14 + function_13


def function_19(function_10, function_7):
    return function_10 + function_7


def function_20(function_15):
    return function_15


def function_21(function_4):
    return function_4


def function_22(function_10, function_15, function_9):
    return function_15 + function_10 + function_9


def function_23(function_15, function_6):
    return function_15 + function_6


def function_24(function_1, function_12, function_18, function_23, function_5):
    return function_12 + function_1 + function_18 + function_23 + function_5


def function_25(function_14, function_18):
    return function_18 + function_14


def function_26(function_0, function_14, function_2):
    return function_2 + function_14 + function_0


def function_27(function_26):
    return function_26


def function_28(function_17, function_2, function_24, function_25, function_9):
    return function_17 + function_25 + function_24 + function_2 + function_9


def function_29():
    return 29


def function_30(function_13, function_25):
    return function_25 + function_13


def function_31(function_1, function_14, function_15, function_29, function_7):
    return function_15 + function_1 + function_29 + function_7 + function_14


def function_32(function_12, function_22, function_25):
    return function_12 + function_22 + function_25


def function_33(function_3):
    return function_3


def function_34(function_25, function_33, function_4, function_5, function_6):
    return function_4 + function_25 + function_6 + function_33 + function_5


def function_35(function_14, function_17, function_21):
    return function_17 + function_21 + function_14


def function_36(function_33, function_6):
    return function_33 + function_6


def function_37():
    return 19


def function_38(function_33, function_34):
    return function_34 + function_33


def function_39(function_1, function_10, function_25):
    return function_25 + function_1 + function_10


def function_40(function_21, function_24, function_33):
    return function_21 + function_33 + function_24


def function_41(function_18):
    return function_18


#%%

scope = locals()
functions = [scope[f"function_{i}"] for i in range(42) if f"function_{i}" in scope]
large_graph = (
    Composer().update(*functions).link(function_8="function_7", function_9="function_5")
)

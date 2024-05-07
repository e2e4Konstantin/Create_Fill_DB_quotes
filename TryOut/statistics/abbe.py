import numpy as np

def sum_of_differences(values):
    mean = sum(values) / len(values)
    differences = [abs(value - mean) for value in values]
    return sum(differences)



def np_sum_of_differences(values):
    mean = np.mean(values)
    differences = np.abs(values - mean)
    return np.sum(differences)


def _abbe_criterion(signal):
    if signal is None or len(signal) < 2:
        return 0
    differences = np.diff(signal) ** 2
    squares = np.sum(differences)
    # print(squares)
    denominator = np_sum_of_differences(signal)
    # print(denominator)
    return squares // denominator if squares > 0 else 0

def abbe_criterion(signal):
    if signal is None or len(signal) <= 2:
        return 0
    differences = np.diff(signal) ** 2
    squares = np.sum(differences)
    return np.sqrt(squares / (len(signal)-1))


s1 = (51403.63,	51666.67,	51666.67,	41666.67,	41666.67)
s2 = (41250.00,	41250.00,	31979.17,	31840.28,	33506.94)
s3 = (28645.83,	21500.00,	20250.00,	20250.00,	21500.00)
s4 = (4.50,	5.00,	5.50,	6.50,	5.83)
s5 = ( 35.75,	 35.75)
s6 = ( 334.17,	 354.17,	 354.17, 354.17, 354.17)
all_s = [s1, s2, s3, s4, s5, s6]

print(abbe_criterion(s4))

# for _ in all_s:
#     print(abbe_criterion(_))

def romanToInt(s):
    Roman_Dict = {
        'I': 1,
        'V': 5,
        'X': 10,
        "L": 50,
        "C": 100,
        "D": 500,
        "M": 1000
    }
    result = 0
    temp = 1000

    for i in s:
        if Roman_Dict[i] > temp:
            result = result - 2 * temp + Roman_Dict[i]
        else:
            result = result + Roman_Dict[i]

        tmp = Roman_Dict[i]

    return result


romanToInt("MCMXCIV")
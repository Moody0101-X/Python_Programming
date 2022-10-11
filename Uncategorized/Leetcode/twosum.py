
def twosum0(nums: list, target:int) -> list:
    table = {}
    for i, n in enumerate(nums):
        if target - n in table:
            return  [table[target - n], i]
        table[n] = i
    return

def twosum1(LIST: list, INT: int, table={}) -> list | None:
    
    for i, n in enumerate(LIST):
        if INT - n in table:
            return [[tableINT - n], i]
        table[n] = i
    return

print(twosum0([2,7,11,15], 9))

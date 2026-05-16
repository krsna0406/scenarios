s = "a1hec3y4e5"

# step 1: extract and reverse letters
letters = [ch for ch in s if ch.isalpha()]
letters.reverse()

# step 2: build result
result = ""
i = 0

for ch in s:
    if ch.isalpha():
        result += letters[i]
        i += 1
    else:
        result += ch

print(result)
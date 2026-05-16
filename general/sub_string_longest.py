def longest_substring(s):
    seen = set()
    left = 0
    max_len = 0

    for right in range(len(s)):
        # If duplicate, shrink window
        while s[right] in seen:
            seen.remove(s[left])
            left += 1

        seen.add(s[right])
        max_len = max(max_len, right - left + 1)

    return max_len


# print(longest_substring("abcDEEEabcbb"))  # 3

print(longest_substring("pwwkew"))

print(longest_substring("cadbzabcd"))
def lcs(s1: str, s2: str) -> str:
    p1 = 0
    p2 = 0
    globallongestss = ''
    globallongestsslen = -1

    for p1 in range(len(s1)):
        currp1 = p1
        currlongestss = ''
        currlongestsslen = -1
        for p2 in range(len(s2)):
            if(s1[currp1] == s2[p2]):
                currlongestss += s1[currp1]
                if(currlongestsslen == -1):
                    currlongestsslen = 1
                else:
                    currlongestsslen += 1
            # else:
            #     continue
            currp1 += 1
            if(currp1 >= len(s1)):
                break
        if(currlongestsslen > globallongestsslen):
            globallongestsslen = currlongestsslen
            globallongestss = currlongestss

    return globallongestss


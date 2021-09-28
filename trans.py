o = ["Nested Loop", "Seq Scan", "Materialize", "Aggregate", "Sort", "Hash", "Hash Join", "Index Only Scan", "Merge Join", "Index Scan"]
cnt = 0
r = []
with open("input.txt","r") as f:
    with open("output.txt", "w") as f_out:
        try:
            for line in f:           
                if cnt % 3 == 0:
                    r = eval(line)
                    f_out.write(str(r) + "\n")
                elif cnt % 3 == 1:
                    q = eval(line)
                    print(q)
                    backup = [0 for _ in range(0, 10)]
                    for count, i in enumerate(q):
                        if count > len(r):
                            break
                        flag = 0
                        for element in range(0, len(o)):
                            if o[element] == r[count - 1]:
                                backup[element] += i
                                flag = 1
                                break
                        if flag == 0:
                            print("Fault !")
                    q = q[len(r):]
                    q = backup + q
                    f_out.write(str(q) + "\n")
                elif cnt % 3 == 2:
                    pass
                    f_out.write(str(line))
        except:
            pass

        cnt += 1

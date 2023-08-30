
printer = "vnr;heiti;magn"
for i in range(1, 1000000):
    if i%100 == 0:
        print(f"Still going, i: {i}")
    printer += f"i{i};product name;569\n"

with open("large.csv", 'w') as f:
    f.write(printer)

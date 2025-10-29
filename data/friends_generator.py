import random

n = 500
min_friends = 2
max_friends = 300
min_age = 20
max_age = 60

friends_count = [["PID", "PNAME", "AGE", "FRIENDS"]]

for i in range(n):
    friend = [
            i,
            f"Person-{i}",
            random.randint(min_age, max_age),
            random.randint(min_friends, max_friends)
            ]
    friends_count.append(friend)

with open("friends_count.txt", 'w') as file:
    for item in friends_count:
        file.write(f"{item}\n")


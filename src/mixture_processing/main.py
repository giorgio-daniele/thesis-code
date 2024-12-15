from collections import Counter
import random

# Generate a list of 100 sublists, each containing 3 random names
names_pool = ["Alice", "Bob", "Charlie", "David", "Eve", "Frank", "Grace", "Hannah", 
              "Isaac", "Jack", "Katie", "Liam", "Mona", "Nina", "Oliver", "Paul", 
              "Quincy", "Rachel", "Steve", "Tracy"]

# Generate a list of sublists where each sublist has 3 random names
list_of_lists = [[random.choice(names_pool), random.choice(names_pool), random.choice(names_pool)] for _ in range(100)]

# Initialize counters for each position (first, second, third)
first_position = Counter()
second_position = Counter()
third_position = Counter()

# Count how often each name appears in each position (first, second, third)
for sublist in list_of_lists:
    first_position[sublist[0]] += 1
    second_position[sublist[1]] += 1
    third_position[sublist[2]] += 1

# Print the frequency of names in each position
print("Frequency of names in the first position:")
for name, freq in first_position.most_common():
    print(f"{name}: {freq}")

print("\nFrequency of names in the second position:")
for name, freq in second_position.most_common():
    print(f"{name}: {freq}")

print("\nFrequency of names in the third position:")
for name, freq in third_position.most_common():
    print(f"{name}: {freq}")

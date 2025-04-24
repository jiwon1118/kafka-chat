from tqdm import tqdm
import time

for i in tqdm(range(1000)):
    print(i, end="")
    time.sleep(0.01)

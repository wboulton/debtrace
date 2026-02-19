import sqlite3
import requests
import subprocess
from multiprocessing import Pool

def get_distinct_packages(db_file="/home/cyberg/debtrace/data/debtrace.db"):
    conn = sqlite3.connect(db_file)
    cur = conn.cursor()

    cur.execute("""
        SELECT DISTINCT Package
        FROM Publish_Packages
    """)
    
    distinct_packages = [row[0] for row in cur.fetchall()]
    distinct_pairs = []
    for package in distinct_packages:
        cur.execute(f"SELECT DISTINCT version FROM Publish_Packages WHERE Package='{package}'")
        distinct_pairs.append((package, [row[0] for row in cur.fetchall()]))
    
    conn.close()
    
    return distinct_pairs

def find_path(pair):
    paths = []
    for version in pair[1]:
        result = subprocess.run(['/home/cyberg/debold/debtrace',pair[0],version], capture_output=True, text=True)
        paths.append(result.stdout)
    return paths

# print(get_distinct_packages())

"""
for pair in distinct:
    for version in pair[1]:
        subprocess.run(['/home/cyberg/debold/debtrace', pair[0], version])

for i in range(10):
    pair = distinct[i]
    for version in pair[1]:
        subprocess.run(['/home/cyberg/debold/debtrace', pair[0], version])
"""

if __name__ == '__main__':
    # Clear results.txt before starting
    with open("results.txt", "w") as f:
        f.write("")
    
    distinct = get_distinct_packages()
    found = 0
    n = 0

    for pair in distinct:
        with open("results.txt", "a") as f:
            for version in pair[1]:
                n += 1
                result = subprocess.run(['/home/cyberg/debold/test', pair[0], version], capture_output=True, text=True)
                not_found = 'No path found' in result.stdout
                f.write(f"path for {pair} found: {not not_found}\n")
                if (not not_found):
                    found += 1
                    prefix = 'path found from source to buildinfo to package:'
                    if prefix in result.stdout:
                        path_content = result.stdout.split(prefix, 1)[1].strip()
                        f.write(f"{path_content}\n\n")
                    else:
                        f.write(f"{result.stdout}\n\n")
        with open("count.txt", "w") as g:
            g.write(f"found: {found}\ntested: {n}\n")
            g.write(f"percent found: {found/n * 100}")
    


"""
path for ('aces3-data', ['3.0.8-4', '3.0.8-5.1', '3.0.8-6', '3.0.8-7']) found: False
path for ('aces3-data', ['3.0.8-4', '3.0.8-5.1', '3.0.8-6', '3.0.8-7']) found: False
path for ('aces3-data', ['3.0.8-4', '3.0.8-5.1', '3.0.8-6', '3.0.8-7']) found: False
path for ('aces3-data', ['3.0.8-4', '3.0.8-5.1', '3.0.8-6', '3.0.8-7']) found: False
"""
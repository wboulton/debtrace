import sqlite3
import requests
import subprocess
from multiprocessing import Pool, cpu_count

BINARY_PATH = "/home/cyberg/debold/debold"
DATABASE_PATH = "/home/cyberg/debtrace/debtrace_old.db"

def get_distinct_packages(db_file=DATABASE_PATH):
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
        result = subprocess.run([BINARY_PATH, pair[0], version], capture_output=True, text=True)
        paths.append(result.stdout)
    return paths


def run_test(task):
    package, version = task
    result = subprocess.run(
        [BINARY_PATH, package, version],
        capture_output=True,
        text=True
    )
    output = result.stdout
    not_found = 'No path found' in output
    path_content = None
    if not not_found:
        prefix = 'path found from source to buildinfo to package:'
        if prefix in output:
            path_content = output.split(prefix, 1)[1].strip()
        else:
            path_content = output.strip()
    return package, version, not not_found, path_content

# print(get_distinct_packages())

"""
for pair in distinct:
    for version in pair[1]:
        subprocess.run(['/home/cyberg/debold/debold', pair[0], version])

for i in range(10):
    pair = distinct[i]
    for version in pair[1]:
        subprocess.run(['/home/cyberg/debold/debold', pair[0], version])
"""

if __name__ == '__main__':
    # Clear results.txt before starting
    with open("results.txt", "w") as f:
        f.write("")
    
    distinct = get_distinct_packages()
    tasks = []
    for package, versions in distinct:
        for version in versions:
            tasks.append((package, version))

    found = 0
    n = 0
    process_count = max(1, cpu_count() - 1)

    with Pool(processes=process_count) as pool:
        with open("results.txt", "a") as f:
            for package, version, is_found, path_content in pool.imap_unordered(run_test, tasks, chunksize=20):
                n += 1
                f.write(f"path for ({package}, {version}) found: {is_found}\n")
                if is_found:
                    found += 1
                    if path_content:
                        f.write(f"{path_content}\n\n")

                with open("count.txt", "w") as g:
                    g.write(f"found: {found}\ntested: {n}\n")
                    g.write(f"percent found: {found/n * 100}")
    


"""
Low success rate, but the results seem right, I think the buildinfos are missing older packages 
(Publish_Packages goes back further than buildinfo_table)
"""
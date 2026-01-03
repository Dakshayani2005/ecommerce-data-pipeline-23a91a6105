import subprocess
import sys

def main():
    if "--help" in sys.argv:
        print("Run project tests using pytest")
        sys.exit(0)

    result = subprocess.run(
        [sys.executable, "-m", "pytest"]
    )

    if result.returncode != 0:
        sys.exit("Tests failed")

    print("All tests passed")

if __name__ == "__main__":
    main()

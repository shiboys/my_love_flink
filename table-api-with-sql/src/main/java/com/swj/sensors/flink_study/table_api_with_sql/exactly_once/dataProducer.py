import json
import time


def main():
    for i in range(20):
        jsonObj = {"value": str(i+1)}
        print(json.dumps(jsonObj))
        time.sleep(1)


if __name__ == '__main__':
    main()

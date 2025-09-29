from config import BathingWatersApi


def main():
    api_config = BathingWatersApi()

    print(api_config.health())


if __name__ == "__main__":
    main()

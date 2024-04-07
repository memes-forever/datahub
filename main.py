from utils import DataHubUtils


if __name__ == "__main__":
    dhu = DataHubUtils()

    # load config
    descs = dhu.load_yaml("./configs/descriptions.yaml")

    # rename description, if need
    dhu.change_datasets_descriptions(descs)

    # save
    descs = dhu.get_datasets_descriptions()
    dhu.save_yaml(descs, "./configs/descriptions.yaml")

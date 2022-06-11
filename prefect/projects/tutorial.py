from prefect import Client


if __name__ == '__main__':
    client = Client()
    client.create_project(project_name="tutorial")

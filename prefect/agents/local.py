from prefect.agent.local import LocalAgent

agent = LocalAgent()

if __name__ == '__main__':
    agent.start()

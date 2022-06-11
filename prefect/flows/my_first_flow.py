from prefect import Flow

from tasks.say_hello import say_hello

with Flow("My first flow!") as flow:
    say_hello()

if __name__ == '__main__':
    flow.register(
        project_name="tutorial",

        # Only bump the version if the flow has changed, instead of upon every deployment
        idempotency_key=flow.serialized_hash()
    )

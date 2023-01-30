from main import Processor
import sentry_sdk

if __name__ == "__main__":
    sentry_sdk.init(
        dsn="https://a965c86590964e07b4b759d66633bb62@sentry.housestats.co.uk/3",
        traces_sample_rate=1.0
    )
    print("loading")
    processor = Processor()
    print("Running")
    processor.main_loop()
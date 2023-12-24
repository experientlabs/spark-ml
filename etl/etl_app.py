import argparse
import datetime
import importlib
import time
from etl.logging_utils.logging_util import setup_logger


def run(job_name: str) -> None:
    logger = setup_logger()
    """Run given job name"""
    start = time.time()
    try:
        module = importlib.import_module(f"jobs.{job_name}")
        module.run(job_name)
        end = time.time()
        logger.info(f"Execution of job {job_name} took {end - start} seconds")
    except Exception as e:
        logger.info(
            str(datetime.datetime.now())
            + "____________ Abruptly Exited________________"
        )
        raise Exception(f"Exception::Job {job_name} failed with msg {e}")


def setup_parser():
    parser = argparse.ArgumentParser(description="Run Spark jobs with different dataset sizes.")
    parser.add_argument("job_name", type=str, help="Name of the Spark job to run.")
    return parser


if __name__ == "__main__":
    arg_parser = setup_parser()
    args = arg_parser.parse_args()
    run(args.job_name)

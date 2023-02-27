from dagster_aws.s3.sensor import get_s3_keys
from dagster import sensor

@sensor(job=my_job)
def my_s3_sensor(context):
    since_key = context.cursor or None
    new_s3_keys = get_s3_keys("my_s3_rfp_bucket", since_key=since_key)
    if not new_s3_keys:
        return SkipReason("No new s3 files found for bucket my_s3_rfp_bucket.")
    last_key = new_s3_keys[-1]
    run_requests = [RunRequest(run_key=s3_key, run_config={}) for s3_key in new_s3_keys]
    context.update_cursor(last_key)
    return run_requests
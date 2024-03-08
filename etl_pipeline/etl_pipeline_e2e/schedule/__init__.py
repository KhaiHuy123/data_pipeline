from ..job import reload_data, update_pipeline_job
from dagster import ScheduleDefinition

'''

Crontab Syntax
+---------------- minute (0 - 59)
|  +------------- hour (0 - 23)
|  |  +---------- day of month (1 - 31)
|  |  |  +------- month (1 - 12)
|  |  |  |  +---- day of week (0 - 6) (Sunday is 0 or 7)
|  |  |  |  |
*  *  *  *  *  command to be executed

* means all values are acceptable

'''

reload_data_schedule = ScheduleDefinition(
    job=reload_data,
    cron_schedule="59 23 * * 7",  # every day at midnight weekend day
)

update_pipeline_schedule = ScheduleDefinition(
    job=update_pipeline_job,
    cron_schedule="59 5 1 4 *",  # every 1st of the April at 5:59 AM
)







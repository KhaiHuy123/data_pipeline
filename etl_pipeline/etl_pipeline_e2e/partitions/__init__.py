from dagster import MonthlyPartitionsDefinition, WeeklyPartitionsDefinition
from ..assets import constant

start_date = constant.START_DATE
end_date = constant.END_DATE

monthly_partition = MonthlyPartitionsDefinition(
  start_date=start_date,
  end_date=end_date
)

weekly_partition = WeeklyPartitionsDefinition(
  start_date=start_date,
  end_date=end_date
)


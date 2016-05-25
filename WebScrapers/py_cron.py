from crontab import CronTab
#init cron
cron = CronTab(user=True)

#add new cron job
job  = cron.new(command='/home/alkal/Documents/FantasyHockey/WebScrape/populate_daily_scores.py')

#job settings
job.minute.during(0,50).every(2)

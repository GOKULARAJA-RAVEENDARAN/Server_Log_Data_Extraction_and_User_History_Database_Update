#AUTHOR:GOKULARAJA R
use mini_project;

#What is the average number of emails received per day?
SELECT 
    AVG(email_count) AS avg_emails_per_day
    FROM (
    SELECT DATE(date) AS email_date, COUNT(*) AS email_count
    FROM user_history
    GROUP BY DATE(date)
    ) AS daily_emails;

#Total Number of email received?
SELECT COUNT(*) AS total_emails FROM user_history;

#Find users who sent emails on more than one day
SELECT email, COUNT(DISTINCT DATE(date)) AS active_days
FROM user_history
GROUP BY email
HAVING active_days > 1;

#Which Users sent more than 3 email?
SELECT email, COUNT(*) AS email_count
FROM user_history
GROUP BY email
HAVING email_count > 3;

#Find dates with more than 5 emails.
SELECT DATE(date) AS email_date, COUNT(*) AS email_count
FROM user_history
GROUP BY DATE(date)
HAVING email_count > 5
ORDER BY email_date;

#Which email address sent the most emails?
SELECT 
    email, COUNT(*) AS total
FROM
    user_history
GROUP BY email
ORDER BY total DESC
LIMIT 1;

#Which domain has the most emails?
SELECT 
    SUBSTRING_INDEX(email, '@', -1) AS domain, 
    COUNT(*) AS email_count
FROM user_history
GROUP BY domain
ORDER BY email_count DESC
LIMIT 1;

#Find dates with more than 5 emails?
SELECT DATE(date) AS email_date, COUNT(*) AS email_count
FROM user_history
GROUP BY DATE(date)
HAVING email_count > 2
ORDER BY email_date;

#Find email addresses that only sent one email
SELECT email
FROM user_history
GROUP BY email
HAVING COUNT(*) = 1;

#Get the oldest email in the database
SELECT * FROM user_history
ORDER BY date ASC
LIMIT 1;

#Get the most recent email sent
SELECT * FROM user_history
ORDER BY date DESC
LIMIT 1;



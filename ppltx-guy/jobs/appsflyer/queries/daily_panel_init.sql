/*
{description}

{run_time}
*/

CREATE OR REPLACE TABLE
  `{project_id}.{dataset_dst}.{table_dst}` --  `subpltx-dev.g_appsflyer.daily_campaign_panel`
(
    campaign               STRING	options(description='User unique ID'),
    dt                     DATE	    options(description='Activity date'),
    media_source           STRING	options(description='platform of the user'),
    is_install             STRING   options(description='is the campaign for installs'),
    t_events               INTEGER  options(description='total events'),
    t_installs             INTEGER	options(description='total installs'),
    t_af_purchase          INTEGER  options(description='total appsflyer purchases'),
    t_SessionStart         INTEGER	options(description='total SessionStart events'),
    t_SessionEnd           INTEGER	options(description='total SessionEnd events'),
    t_TournamentStart      INTEGER	options(description='total tournaments started'),
    t_TournamentFinished   INTEGER	options(description='total tournaments finished'),
    t_ios_users            INTEGER	options(description='total IOS Users'),
    t_android_users        INTEGER	options(description='total Android Users'),
    t_users_from_facebook  INTEGER	options(description='total users from facebook'),
    t_users_from_Google    INTEGER	options(description='total users from google'),
    t_users_from_TikTok    INTEGER	options(description='total users from tiktok'),
    t_users_from_Organic   INTEGER	options(description='total organic users'),
    revenue                FLOAT64	options(description='total revenue'),
    cost                   FLOAT64	options(description='total cost')
)

PARTITION BY
{partition_att} --  dt
OPTIONS(description="{description}" )
AS

SELECT
  campaign,
  dt,
  MAX(af_channel) AS media_source,
  CASE
    WHEN  event_name = 'install' THEN event_name
    ELSE 'non_install' END AS is_install,
  count(1) as t_events,
  SUM(CASE WHEN  event_name = 'install' THEN 1 END) AS t_installs,
  SUM(CASE WHEN  event_name = 'af_purchase' THEN 1 END) AS t_af_purchase,
  SUM(CASE WHEN  event_name = 'SessionStart' THEN 1 END) AS t_SessionStart,
  SUM(CASE WHEN  event_name = 'SessionEnd' THEN 1 END) AS t_SessionEnd,
  SUM(CASE WHEN  event_name = 'TournamentStart' THEN 1 END) AS t_TournamentStart,
  SUM(CASE WHEN  event_name = 'TournamentFinished' THEN 1 END) AS t_TournamentFinished,
  SUM(CASE WHEN  platform = 'ios' THEN 1 END) AS t_ios_users,
  SUM(CASE WHEN  platform = 'android' THEN 1 END) AS t_android_users,
  SUM(CASE WHEN  af_channel = 'Facebook' THEN 1 END) AS t_users_from_facebook,
  SUM(CASE WHEN  af_channel = 'Google' THEN 1 END) AS t_users_from_Google,
  SUM(CASE WHEN  af_channel = 'TikTok' THEN 1 END) AS t_users_from_TikTok,
  SUM(CASE WHEN  af_channel = 'Organic' THEN 1 END) AS t_users_from_Organic,
  ROUND(SUM(event_revenue)) AS revenue,
  ROUND(SUM(af_cost_value)) AS cost,
FROM
  `{project_id}.{dataset_src}.{table_src}` --`subpltx-dev.g_appsflyer.fact`
WHERE
{partition_att} <= '{date}'
GROUP BY ALL

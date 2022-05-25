CREATE TABLE visits_goals_view
(
  "VisitID" UInt64 ,
  "Date" Date,
  "GoalID" UInt32,
  "sources" String,
  "Region_name" String,
  "Host_type" String,
  "Mrf" String,
  "Goal_name" String
)

ENGINE = MergeTree()
PARTITION BY toYYYYMM(Date)
ORDER BY Date
AS
WITH visits_mrf AS(
	SELECT
	va.VisitID ,
	va.StartURL ,
	va.LastsignUTMSource,
	va.LastsignUTMMedium,
	va.Date ,  
	arrayFilter(x -> x IN [56035891,56036062,230160567,231442342] , va.GoalsID) AS GoalIDs,
  multiIf(
  
               match( LastsignTrafficSource, 'referral'),
             --OR match (LastSocialNetwork, 'referral'),
             --OR match (FirstTrafficSource, 'referral'),
            'Реферальный трафик',
            
           	match( LastsignTrafficSource, 'organic')
           	OR match (LastSocialNetwork, 'organic'),
           	--OR match (FirstTrafficSource, 'organic'),
            'Органический поиск',
            
            match( LastsignTrafficSource, 'direct')
            AND match( LastSocialNetwork, 'direct')
            AND match(LastsignUTMMedium, '^$') ,
            'Без ссылки/ Прямой',
            
            
  			match( LastsignUTMSource,'^(2gis)$')
            AND match( LastsignUTMMedium, '^(cpc|banner|betta|tgb|cpm)$') ,
            --AND match( LastsignTrafficSource, '^ad$'),
            'cpc-2gis',
           match( LastsignUTMSource, '^(google|google_rs)$')
           AND match( LastsignUTMMedium, '^(cpc|banner|betta|tgb|cpm)$')
           AND match( LastsignTrafficSource, '^ad$'),
            'cpc-google',

            match( LastsignUTMSource, '^(media|_media|dbm|dfa|dd_)'),
           'Медийная реклама',
            match( LastsignUTMSource, 'social_b2c|social_b2b')
          OR match( LastsignUTMMedium, 'social_b2c|social_b2b'),
          'Социальные сети',
            
            --match(LastsignTrafficSource,'ad')
        	match(LastsignUTMMedium, '(cpc|banner|betta|tgb|cpm)')
            AND match(LastsignTrafficSource, '^ad')
            AND match(LastsignUTMSource,'^yandex')
            AND match(LastAdvEngine,'ya_direct'),
            'cpc-yandex',

            match( LastsignUTMSource, '^(gmb|mytarget|target|facebook|vk.com|vk|vkontakte|yaspr)$')
            AND match( LastsignUTMMedium, '^(cpc|banner|betta|tgb|cpm)$')
            AND match( LastsignTrafficSource, 'ad$'),
            'cpc-others',

         'Other sources') as sources,
			  md.Region_name,
			  md.Host_type,
			  md.Mrf
	FROM 
				db_metrica.visits_all va 
	--LEFT ARRAY JOIN va.GoalsID as GoalID
	INNER JOIN db_metrica.mrf_dict md 
		ON substring(va.StartURL, position(va.StartURL,'://') + 3, position(va.StartURL,'.ru/')-position(va.StartURL,'://')) = md.Host_type
		),

all_visits_goals as(
	SELECT 
		visits_mrf.VisitID , 
		visits_mrf.Date ,
		GoalID,
		visits_mrf.sources,
		visits_mrf.Region_name,
		visits_mrf.Host_type,
		visits_mrf.Mrf
		
	FROM visits_mrf
	LEFT ARRAY JOIN visits_mrf.GoalIDs as GoalID
)

SELECT all_visits_goals.*, ga.Goal_name 
FROM all_visits_goals
LEFT JOIN db_metrica.goals_all ga 
	ON all_visits_goals.GoalID = ga.Goal_id 



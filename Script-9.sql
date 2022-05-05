
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
	va.StartURL ,
	va.VisitID , 
	va.Date ,  
	arrayFilter(x -> x IN [56035891,56036062,230160567,231442342] , va.GoalsID) AS GoalIDs,
	multiIf(  
				match(LastsignUTMSource,'^(yandex|yandex_rs)$') 
			    	AND match( LastsignUTMMedium, '^(cpc|banner|betta|tgb|cpm)$') 
			    	OR match( LastsignUTMSource, '^(yabs.yandex.ru)$') 
			    	AND match( LastsignUTMMedium, '^referral$'),
			    	'cpc-yandex',
		    	 match( LastsignUTMSource, '^(google|google_rs)$')
			 		AND match( LastsignUTMMedium, '^(cpc|banner|betta|tgb|cpm)$')
			  		OR match( LastsignUTMSource, '^(googleads.g.doubleclick.net)$')
			  		AND match( LastsignUTMMedium, '^referral$'),
			  		'cpc-google',
		  		 match( LastsignUTMSource,'^(2gis)$') 
			    	AND match( LastsignUTMMedium, '^(cpc|banner|betta|tgb|cpm)$') 
			    	OR match( LastsignUTMSource, '^(link.2gis.ru)$') 
			    	AND match( LastsignUTMMedium, '^referral$'),
			    	'cpc-2gis',
			   	 match( LastsignUTMSource, '^(gmb|mytarget|target|facebook|vk.com|vk|vkontakte|yaspr)$')
		  			AND match( LastsignUTMMedium, '^(cpc|banner|betta|tgb|cpm)$')
		  			OR match( LastsignUTMSource, '^(click.mail.ru|link.2gis.ru)$')
		  			AND match( LastsignUTMMedium, '^referral$'),
		  			'cpc-others',
		  		 match( LastsignUTMSource, 'direct') 
		    		AND match( LastsignUTMMedium, ''),
		    		'Без ссылки/ Прямой',
		    	 match( LastsignUTMMedium, 'organic')
		  			OR match( LastsignUTMMedium, 'referral')
		  			AND match( LastsignUTMSource,'^yandex.ru'),
		  			'Органический поиск',
		  		 match( LastsignUTMSource, '^(media|_media|dbm|dfa|dd_)'),
		  		 'Медийная реклама',
		  		 match( LastsignUTMMedium, ' referral')
		  			AND NOT match( LastsignUTMSource, '^(click.mail.ru|googleads.g.doubleclick.net|yabs.yandex.ru|link.2gis.ru)$'),
		  			'Реферальный трафик',
		  		 match( LastsignUTMSource, 'social_b2c|social_b2b') 
		    	OR match( LastsignUTMMedium, 'social_b2c|social_b2b'),
		    	'Социальные сети',
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

SELECT all_visits_goals.*,
	ga.Goal_name
FROM all_visits_goals
LEFT JOIN db_metrica.goals_all ga 
	ON all_visits_goals.GoalID = ga.Goal_id 
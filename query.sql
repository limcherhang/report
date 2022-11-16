SET time_zone = '+00:00';
SET @date1 = '2022-09-30';

SELECT 
	@date1, gid, SUM(total_bet) AS bets, SUM(total_win) AS wins,
	SUM(total_round) AS rounds, SUM(valid_bet) AS valid_bet, SUM(income) AS income, 'ALL' AS currency, 
	FORMAT((SUM(total_bet)-SUM(total_win))/SUM(total_bet)*100 ,2) AS kill_rate, onlinetime, 
	SUM(total_rake) AS rakes, SUM(room_fee) AS room_fee, brand, SUM(IF(`(non)first`=1, 1, 0)) AS player_first,
    SUM(IF(`(non)first`=1, total_bet, 0)) AS bets_first
FROM
(
	SELECT 
		stat.gid, user_list.ownerid AS oid, user_list.parentid AS pid, stat.uid, 
		SUM(total_bet/fx_rate.rate) AS total_bet, SUM(total_win/fx_rate.rate) AS total_win, 
		SUM(total_round) AS total_round, SUM(total_bet/fx_rate.rate) AS valid_bet,
		SUM(total_bet/fx_rate.rate)-SUM(total_win/fx_rate.rate) AS income, user_list.currency,
		0 AS total_rake, 0 AS room_fee, game_info.onlinetime, game_info.brand, pf.firstGamingTime,
		IF(pf.firstGamingTime>=@date1 AND pf.firstGamingTime<DATE_ADD(@date1,INTERVAL 1 DAY),1,0) AS `(non)first`
	FROM
	cypress.statistic_user_by_game AS stat
	JOIN MaReport.game_info ON game_info.gid = stat.gid
	JOIN cypress.user_list ON user_list.id = stat.uid
	JOIN cypress.parent_list ON parent_list.id = user_list.parentid
	JOIN cypress.parent_list AS owner_list ON owner_list.id = user_list.ownerid
	JOIN cypress.fx_rate ON user_list.currency=fx_rate.short_name
	JOIN dataWarehouse.player_firstTime_lastTime_gaming_info AS pf ON owner_list.id=pf.oid AND parent_list.id=pf.pid AND user_list.id=pf.uid AND pf.gid=game_info.gid
	WHERE `date`>=@date1 AND `date`<DATE_ADD(@date1, INTERVAL 1 DAY) AND parent_list.istestss=0 AND owner_list.istestss=0 AND stat.gid=1
	GROUP BY stat.gid, user_list.ownerid, user_list.parentid, stat.uid LIMIT 10000
) AS tb;

-- usergametoken log
SET @date1 = '2022-10-31 00:00:00';
SELECT * FROM MaReport.user_gametoken_log WHERE `date`>=@date1 AND `date`<DATE_ADD(@date1, INTERVAL 1 HOUR) ORDER BY gametoken;

SELECT * FROM cypress.user_list;


SELECT 
            owner_list.id AS oid, parent_list.id AS pid, user_list.id, 
            IF(pf.firstGamingTime>=@date1 AND pf.firstGamingTime<DATE_ADD(@date1,INTERVAL 1 DAY),1,0),
            MIN(starttime), MAX(endtime)
        FROM 
        MaReport.user_gametoken_log AS ugl
        JOIN cypress.user_list ON user_list.userid=ugl.userid
        JOIN cypress.parent_list ON user_list.parentid = parent_list.id
        JOIN cypress.parent_list AS owner_list ON owner_list.id = user_list.ownerid
        JOIN MaReport.game_info ON game_info.game_code=ugl.game_code
        JOIN dataWarehouse.player_firstTime_lastTime_gaming_info AS pf ON owner_list.id=pf.oid AND parent_list.id=pf.pid AND user_list.id=pf.uid AND pf.gid=game_info.gid
        WHERE ugl.`date`>=@date1 AND ugl.`date`<DATE_ADD(@date1,INTERVAL 1 DAY) AND ugl.game_code = 1
        GROUP BY owner_list.id, parent_list.id, user_list.id;
        
-- report by game daily
SELECT * FROM MaReport.report_by_game_daily WHERE `date` = @date1 AND currency='ALL' ORDER BY `no`;

SET @date1 = '2022-11-02';

SELECT 
	*
FROM 
MaReport.report_daily_user
WHERE `date` >=@date1 AND `date`<DATE_ADD(@date1, INTERVAL 30 DAY) AND currency='VND'
GROUP by oid, pid, gid, uid;

SELECT * FROM MaReport.game_info WHERE game_name_tw='跳高高';

-- 驗證 uid不在dataWarehouse
SELECT 
	*
FROM
(
	SELECT 
		stat.uid
	FROM
	cypress.statistic_user_by_game AS stat
	JOIN MaReport.game_info ON game_info.gid=stat.gid
	JOIN cypress.user_list ON stat.uid = user_list.id
	JOIN cypress.parent_list ON parent_list.id = user_list.parentid
	JOIN cypress.parent_list AS owner_list ON owner_list.id = user_list.ownerid
	JOIN cypress.fx_rate ON user_list.currency = fx_rate.short_name
    JOIN dataWarehouse.player_firstTime_lastTime_gaming_info AS pf ON owner_list.id=pf.oid AND parent_list.id=pf.pid AND user_list.id=pf.uid AND pf.gid=game_info.gid
	WHERE `date` >= @date1 AND `date` < DATE_ADD(@date1, INTERVAL 1 DAY) AND 
	parent_list.istestss = 0 AND owner_list.istestss = 0 AND stat.gid = 51 AND user_list.currency='CNY'
	GROUP BY stat.uid, parent_list.id, owner_list.id
) AS tb
RIGHT JOIN
(
	SELECT 
		stat.uid
	FROM
	cypress.statistic_user_by_game AS stat
	JOIN MaReport.game_info ON game_info.gid=stat.gid
	JOIN cypress.user_list ON stat.uid = user_list.id
	JOIN cypress.parent_list ON parent_list.id = user_list.parentid
	JOIN cypress.parent_list AS owner_list ON owner_list.id = user_list.ownerid
	JOIN cypress.fx_rate ON user_list.currency = fx_rate.short_name
    -- JOIN dataWarehouse.player_firstTime_lastTime_gaming_info AS pf ON owner_list.id=pf.oid AND parent_list.id=pf.pid AND user_list.id=pf.uid AND pf.gid=game_info.gid
	WHERE `date` >= @date1 AND `date` < DATE_ADD(@date1, INTERVAL 1 DAY) AND 
	parent_list.istestss = 0 AND owner_list.istestss = 0 AND stat.gid = 51 AND user_list.currency='CNY'
	GROUP BY stat.uid, parent_list.id, owner_list.id
) AS tb2
ON tb.uid=tb2.uid WHERE tb.uid IS NULL;

SELECT * FROM dataWarehouse.player_firstTime_lastTime_gaming_info WHERE uid = 171696482 AND gid = 51;

SELECT
            uid, first_nonfirst, MIN(starttime), MAX(endtime)
        FROM
        (
            SELECT 
                owner_list.id AS oid, parent_list.id AS pid, user_list.id AS uid, 
                IFNULL(IF(pf.firstGamingTime>=@date1 AND pf.firstGamingTime<DATE_ADD(@date1,INTERVAL 1 DAY),1,0), 0) AS first_nonfirst,
                MIN(starttime) AS starttime, MAX(endtime) AS endtime
            FROM 
            MaReport.user_gametoken_log AS ugl
            JOIN cypress.user_list ON user_list.userid=ugl.userid
            JOIN cypress.parent_list ON user_list.parentid = parent_list.id
            JOIN cypress.parent_list AS owner_list ON owner_list.id = user_list.ownerid
            JOIN MaReport.game_info ON game_info.game_code=ugl.game_code
            LEFT JOIN dataWarehouse.player_firstTime_lastTime_gaming_info AS pf ON owner_list.id=pf.oid AND parent_list.id=pf.pid AND user_list.id=pf.uid AND pf.gid=game_info.gid
            WHERE ugl.`date`>=@date1 AND ugl.`date`<DATE_ADD(@date1,INTERVAL 1 DAY) AND ugl.game_code = 'GO02'
            GROUP BY owner_list.id, parent_list.id, gametoken
        ) AS tb GROUP BY oid, pid, uid;

-- ------------
SELECT
	tb.uid, SUM(play_time) AS play_time, 
    CASE (firstGamingTime)
    WHEN firstGamingTime>=@date1 AND firstGamingTime<DATE_ADD(@date1,INTERVAL 1 DAY) THEN 1
    ELSE 0
    END AS first_nonfirst
FROM
(
	SELECT 
		owner_list.id AS oid, parent_list.id AS pid, user_list.id AS uid, gid,
		-- IFNULL(IF(pf.firstGamingTime>=@date1 AND pf.firstGamingTime<DATE_ADD(@date1,INTERVAL 1 DAY),1,0), 0) AS first_nonfirst,
		MAX(endtime)-MIN(starttime) AS play_time
	FROM 
	MaReport.user_gametoken_log AS ugl
	JOIN cypress.user_list ON user_list.userid=ugl.userid
	JOIN cypress.parent_list ON user_list.parentid = parent_list.id
	JOIN cypress.parent_list AS owner_list ON owner_list.id = user_list.ownerid
	JOIN MaReport.game_info ON game_info.game_code=ugl.game_code
	WHERE ugl.`date`>=@date1 AND ugl.`date`<DATE_ADD(@date1,INTERVAL 1 DAY) AND ugl.game_code = '52'
	GROUP BY gametoken
) AS tb
LEFT JOIN dataWarehouse.player_firstTime_lastTime_gaming_info AS pf ON tb.oid=pf.oid AND tb.pid=pf.pid AND tb.uid=pf.uid AND pf.gid=tb.gid
GROUP BY tb.oid, tb.pid, tb.uid;

SELECT 
	user_list.id AS uid,  SUM(play_time)/60 AS play_time
FROM
(
	SELECT userid, MAX(UNIX_TIMESTAMP(endtime))-MIN(UNIX_TIMESTAMP(starttime)) AS play_time
	FROM MaReport.user_gametoken_log
	WHERE `date`>=@date1 AND `date`<DATE_ADD(@date1,INTERVAL 1 DAY) AND game_code='52'
    GROUP BY gametoken
) AS ugl
JOIN cypress.user_list ON user_list.userid=ugl.userid
GROUP BY user_list.userid;

SELECT UNIX_TIMESTAMP('2022-09-01 01:00:00')-UNIX_TIMESTAMP('2022-09-01 00:00:00');
-- 
SELECT 
	stat.gid, user_list.ownerid AS oid, user_list.parentid AS pid, stat.uid, user_list.account,
	SUM(total_bet/fx_rate.rate) AS total_bet, SUM(total_win/fx_rate.rate) AS total_win, 
	SUM(total_round) AS total_round, SUM(total_bet/fx_rate.rate) AS valid_bet,
	SUM(total_bet/fx_rate.rate)-SUM(total_win/fx_rate.rate) AS income, 'ALL',
	0 AS total_rake, 0 AS room_fee, game_info.onlinetime, game_info.brand,
	SUM(total_win/fx_rate.rate) AS player_win
FROM cypress.statistic_user_by_game AS stat
JOIN MaReport.game_info ON game_info.gid=stat.gid
JOIN cypress.user_list ON user_list.id=stat.uid
JOIN cypress.parent_list ON parent_list.id=user_list.parentid
JOIn cypress.parent_list AS owner_list ON owner_list.id=user_list.ownerid
JOIN cypress.fx_rate ON fx_rate.short_name=user_list.currency
WHERE `date`>=@date1 AND `date`<DATE_ADD(@date1, INTERVAL 1 DAY) AND parent_list.istestss=0 AND owner_list.istestss=0 AND stat.gid=51
GROUP BY oid, pid , uid;

SELECT 
	stat.uid, SUM(total_bet) AS total_bet, SUM(total_win) AS total_win, SUM(total_round) AS total_round, 
    SUM(total_bet) AS valid_bet, SUM(total_bet-total_win)/fx_rate.rate AS income,
    SUM(total_rake) AS total_rake, SUM(room_fee) AS room_fee, game_info.onlinetime, SUM(total_win)/fx_rate.rate AS player_win,
    CASE firstGamingTime
    WHEN firstGamingTime>=@date1 AND firstGamingTime<DATE_ADD(@date1,INTERVAL 1 DAY) THEN 1
    ELSE 0
    END AS first_nonfirst
FROM 
(
	SELECT
		gid, uid, SUM(total_bet) AS total_bet, SUM(total_win) AS total_win, SUM(total_round) AS total_round, 0 AS total_rake, 0 AS room_fee
    FROM cypress.statistic_user_by_game
    WHERE `date`>=@date1 AND `date`<DATE_ADD(@date1, INTERVAL 1 DAY) AND gid = 51
    GROUP BY uid
)AS stat
JOIN MaReport.game_info ON game_info.gid=stat.gid
JOIN cypress.user_list ON user_list.id=stat.uid
JOIN cypress.parent_list ON parent_list.id=user_list.parentid
JOIn cypress.parent_list AS owner_list ON owner_list.id=user_list.ownerid
JOIN cypress.fx_rate ON fx_rate.short_name=user_list.currency
LEFT JOIN dataWarehouse.player_firstTime_lastTime_gaming_info AS pf ON owner_list.id=pf.oid AND parent_list.id=pf.pid AND user_list.id=pf.uid AND pf.gid=game_info.gid
WHERE parent_list.istestss=0 AND owner_list.istestss=0 AND user_list.currency IN ('USD', 'USD(0.1)')
GROUP BY uid;

-- 
SELECT * FROM MaReport.game_info LIMIT 400;


-- ___________________dont delete
SELECT rep.uid FROM
(
	SELECT uid FROM
	(
		SELECT uid FROM cypress.statistic_user_by_game AS stat 
		WHERE `date`>='2022-11-02 00:00:00' AND `date`<DATE_ADD('2022-11-02 00:00:00', INTERVAL 1 DAY) AND gid=51
		GROUP BY uid
	) AS stat
) AS rep
LEFT JOIN
(
	SELECT uid FROM
	(
		SELECT uid FROM cypress.statistic_user_by_game AS stat 
		WHERE `date`>='2022-11-01 00:00:00' AND `date`<DATE_ADD('2022-11-01 00:00:00', INTERVAL 1 DAY) AND gid=51
		GROUP BY uid
	) AS stat
) AS `last` ON rep.uid = `last`.uid 
JOIN cypress.user_list ON user_list.id=rep.uid
JOIN cypress.parent_list ON parent_list.id=user_list.parentid
JOIN cypress.parent_list AS owner_list ON owner_list.id=user_list.ownerid
WHERE parent_list.istestss = 0 AND owner_list.istestss = 0 AND `last`.uid IS NULL;
-- ________________dont delete

SELECT * FROM MaReport.report_by_game_daily WHERE `date`='2022-11-02' AND currency='ALL' ORDER BY `no`;

SELECT * FROM cypress.statistic_user_by_tablegame WHERE `date`>='2022-11-02' AND `date`<DATE_ADD('2022-11-02',INTERVAL 1 DAY) AND valid_bet!=total_bet;

SELECT * FROM cypress.user_list;

 SELECT
	SUM(total_bet) AS bets,
	SUM(total_win) AS wins,
	SUM(total_round) AS rounds,
	SUM(valid_bet) AS valid_bet,
	SUM(income) AS income,
	onlinetime,
	SUM(player_win),
	SUM(total_rake) AS rakes,
	SUM(room_fee) AS room_fee,
	COUNT(uid) AS players
	-- SUM(IF(`(non)first`=1, total_bet, 0)) AS bets_first,
FROM
(

		SELECT
			stat.uid, SUM(total_bet)/fx_rate.rate AS total_bet, SUM(total_win)/fx_rate.rate AS total_win,
			SUM(total_round) AS total_round,  SUM(total_bet)/fx_rate.rate AS valid_bet,
			SUM(total_bet-total_win)/fx_rate.rate AS income,
			0 AS total_rake, 0 AS room_fee, game_info.onlinetime, SUM(total_win)/fx_rate.rate AS player_win,
			CASE firstGamingTime
			WHEN firstGamingTime>='2022-11-02 00:00:00' AND firstGamingTime<DATE_ADD('2022-11-02 00:00:00',INTERVAL 1 DAY) THEN 1
			ELSE 0
			END AS first_nonfirst
		FROM
	(
SELECT
	gid, uid, SUM(total_bet) AS total_bet, SUM(total_win) AS total_win, SUM(total_bet_count) AS total_round, 0 AS total_rake, 0 AS room_fee
FROM cypress.statistic_user_by_lottogame
WHERE `date`>='2022-11-02 00:00:00' AND `updated_time`<DATE_ADD('2022-11-02 00:00:00', INTERVAL 25 HOUR) AND gid = 281
GROUP BY uid
) AS stat
JOIN MaReport.game_info ON game_info.gid = stat.gid  JOIN cypress.user_list ON user_list.id = stat.uid  JOIN cypress.parent_list ON parent_list.id = user_list.parentid  JOIN cypress.parent_list AS owner_list ON owner_list.id = user_list.ownerid  JOIN cypress.fx_rate ON user_list.currency=fx_rate.short_name  LEFT JOIN dataWarehouse.player_firstTime_lastTime_gaming_info AS pf ON owner_list.id=pf.oid AND parent_list.id=pf.pid AND user_list.id=pf.uid AND pf.gid=game_info.gid   WHERE parent_list.istestss=0 AND owner_list.istestss=0 AND user_list.currency = 'CNY'
GROUP BY uid
-- # [uid, total_bet, total_win, total_round, valid_bet, income, total_rake, room_fee, onlinetime, player_win, firstnonfirst]
) AS tb;



SELECT SUM(total_bet/rate) FROM cypress.statistic_user_by_lottogame AS stat
JOIN cypress.user_list ON user_list.id=stat.uid
JOIN cypress.parent_list ON parent_list.id=user_list.parentid
JOIN cypress.parent_list AS owner_list ON owner_list.id=user_list.ownerid
JOIN cypress.fx_rate ON fx_rate.short_name=user_list.currency
WHERE `date`>='2022-11-02' AND `updated_time`<'2022-11-03 01:00:00' AND parent_list.istestss=0 AND owner_list.istestss=0 AND stat.gid=869;

SELECT * FROM cypress.genre_list;
SELECT * FROM MaReport.game_info WHERE game_name_tw LIKE 'JP賺金蛋';

SELECT 
	game_info.gid, game_type, game_code, game_name_tw
FROM
MaReport.game_info
JOIN
(
	SELECT DISTINCT gid FROM
	cypress.statistic_user_by_game 
	WHERE `date`>='2022-10-01' AND `date`<DATE_ADD('2022-10-01', INTERVAL 31 DAY)
	UNION
	SELECT DISTINCT gid FROM
	cypress.statistic_user_by_lottogame
	WHERE `date`>='2022-10-01' AND `date`<DATE_ADD('2022-10-01', INTERVAL 31 DAY)
	UNION
	SELECT DISTINCT gid FROM
	cypress.statistic_user_by_tablegame
	WHERE `date`>='2022-10-01' AND `date`<DATE_ADD('2022-10-01', INTERVAL 31 DAY)
) AS stat
ON game_info.gid = stat.gid;
-- 
SELECT rep.uid FROM
(
	SELECT uid FROM
	(
		SELECT uid FROM cypress.statistic_user_by_game AS stat 
		WHERE `date`>='2022-10-01' AND `date`<DATE_ADD('2022-10-01', INTERVAL 31 DAY) AND gid=51
		GROUP BY uid
	) AS stat
) AS rep
LEFT JOIN
(
	SELECT uid FROM
	(
		SELECT uid FROM cypress.statistic_user_by_game AS stat 
		WHERE `date`>='2022-09-01' AND `date`<'2022-10-01' AND gid=51
		GROUP BY uid
	) AS stat
) AS `last` ON rep.uid = `last`.uid 
JOIN cypress.user_list ON user_list.id=rep.uid
JOIN cypress.parent_list ON parent_list.id=user_list.parentid
JOIN cypress.parent_list AS owner_list ON owner_list.id=user_list.ownerid
WHERE parent_list.istestss = 0 AND owner_list.istestss = 0 AND `last`.uid IS NULL;

SELECT uid, user_list.currency FROM
(
	SELECT uid FROM cypress.statistic_user_by_game AS stat 
	WHERE `date`>='2022-10-01' AND `date`<DATE_ADD('2022-10-01', INTERVAL 31 DAY) AND gid=51
	GROUP BY uid
) AS stat
JOIN cypress.user_list ON user_list.id=stat.uid
JOIN cypress.parent_list ON parent_list.id=user_list.parentid
JOIN cypress.parent_list AS owner_list ON owner_list.id=user_list.ownerid
WHERE parent_list.istestss = 0 AND owner_list.istestss = 0; 

SET time_zone = '+00:00';

SELECT * FROM MaReport.report_by_game_daily
WHERE `date` = '2022-11-14' AND currency = 'ALL'
ORDER BY `no`;

SELECT uid, ownerid, parentid, total_bet, total_win, valid_bet, total_bet_count, currency, updated_time FROM
cypress.statistic_user_by_lottogame AS stat
JOIN cypress.user_list ON user_list.id=stat.uid
WHERE `date` >= '2022-11-13' AND `date`<'2022-11-14' AND gid = 869;

SELECT * FROM cypress.user_list
JOIN cypress.parent_list ON user_list.parentid=parent_list.id
JOIN cypress.parent_list AS owner_list ON user_list.ownerid=owner_list.id
WHERE parentid=14695;

SELECT 
	*
FROM MaReport.report_by_owner_daily AS rep
WHERE `date`>= '2022-11-15' AND `date`<DATE_ADD('2022-11-15', INTERVAL 1 DAY) AND currency = 'ALL' ORDER BY `no`;

SELECT * FROM MaReport.owner_info;
SELECT * FROM cypress.parent_list;

SELECT * FROM cypress.fx_rate AS f1
JOIN cypress.fx_rate AS f2 ON f1.country=f2.country AND f1.short_name!=f2.short_name 
WHERE f1.country!='虛擬貨幣'
ORDER BY f1.id;

SELECT * FROM MaReport.report_query_currency;

SELECT * FROM cypress.parent_list WHERE id = 2448;

SELECT 
	`account`, currency, onlinetime
FROM
cypress.parent_list AS owner_list
JOIN MaReport.owner_info ON owner_list.id=owner_info.id
WHERE istestss=0 ;

SELECT * FROM MaReport.owner_info WHERE id = 2448;
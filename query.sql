SET @date1 = '2022-09-30';
SELECT * FROM MaReport.game_info;

SELECT * FROM MaReport.report_by_game_daily WHERE `date`>=@date1 AND `date`<DATE_ADD(@date1,INTERVAL 1 DAY) AND currency='ALL';

-- 一日碼量和局數
SELECT 
	@date1 AS `date`, game_info.game_name_cn, SUM(stat.total_bet) AS total_bet,
    SUM(stat.total_win) AS total_win, total_round
FROM 
cypress.statistic_user_by_game AS stat
JOIN MaReport.game_info ON game_info.gid=stat.gid
JOIN cypress.user_list ON stat.uid = user_list.id
JOIN cypress.parent_list ON parent_list.id = user_list.parentid
JOIN cypress.parent_list AS owner_list ON owner_list.id = user_list.ownerid
JOIN cypress.fx_rate ON user_list.currency = fx_rate.short_name
WHERE `date` >= @date1 AND `date` < DATE_ADD(@date1, INTERVAL 1 DAY) AND parent_list.istestss = 0 AND owner_list.istestss = 0 AND stat.gid = 268;

-- 一小時碼量和局數
SELECT 
	@date1 AS `date`, game_info.game_name_cn, SUM(total_bet) AS total_bet, SUM(total_round) AS total_round
FROM
cypress.statistic_user_by_game AS stat
JOIN MaReport.game_info ON game_info.gid=stat.gid
JOIN cypress.user_list ON stat.uid = user_list.id
JOIN cypress.parent_list ON parent_list.id = user_list.parentid
JOIN cypress.parent_list AS owner_list ON owner_list.id = user_list.ownerid
JOIN cypress.fx_rate ON user_list.currency = fx_rate.short_name
WHERE `date` >= @date1 AND `date` < DATE_ADD(@date1, INTERVAL 1 HOUR) AND parent_list.istestss = 0 AND owner_list.istestss = 0 AND stat.gid = 268;

SELECT * FROM MaReport.game_info LIMIT 500;

-- get_user_bet_win_income
SELECT 
	stat.gid, user_list.ownerid AS oid, user_list.parentid AS pid, stat.uid, 
	SUM(total_bet/fx_rate.rate) AS total_bet, SUM(total_win/fx_rate.rate) AS total_win, 
	SUM(total_round) AS total_round, SUM(total_bet/fx_rate.rate) AS valid_bet,
	SUM(total_bet/fx_rate.rate)-SUM(total_win/fx_rate.rate) AS income, user_list.currency,
	0 AS total_rake, 0 AS room_fee, game_info.onlinetime, game_info.brand
FROM
cypress.statistic_user_by_game AS stat
JOIN MaReport.game_info ON game_info.gid = stat.gid
JOIN cypress.user_list ON user_list.id = stat.uid
JOIN cypress.parent_list ON parent_list.id = user_list.parentid
JOIN cypress.parent_list AS owner_list ON owner_list.id = user_list.ownerid
JOIN cypress.fx_rate ON user_list.currency=fx_rate.short_name
WHERE `date`>=@date1 AND `date`<DATE_ADD(@date1, INTERVAL 1 DAY) AND parent_list.istestss=0 AND owner_list.istestss=0 AND stat.gid=1
GROUP BY stat.gid, user_list.ownerid, user_list.parentid, stat.uid;

SELECT * FROM cypress.statistic_user_by_lottogame AS stat
JOIN MaReport.game_info ON game_info.gid=stat.gid 
JOIN cypress.user_list ON stat.uid = user_list.id
JOIN cypress.parent_list ON user_list.parentid = parent_list.id
JOIN cypress.parent_list AS owner_list ON owner_list.id = user_list.ownerid
WHERE `date`>=@date1 AND `date`<DATE_ADD(@date1, INTERVAL 1 DAY) AND parent_list.istestss=0 AND owner_list.istestss=0 AND stat.gid = 869;

SELECT * FROM cypress.user_list
JOIN dataWarehouse.player_firstTime_lastTime_gaming_info AS pf ON user_list.ownerid=pf.oid AND user_list.parentid=pf.pid AND user_list.id=pf.uid-- AND pf.gid=game_info.gid
WHERE user_list.id IN (128117645, 172911266);-- 這兩個uid沒有記錄到其第一次玩遊戲時間 gametype=sport, gid=869 date=2022-09-30


SELECT 
	stat.gid, user_list.ownerid AS oid, user_list.parentid AS pid, stat.uid, 
	SUM(total_bet/fx_rate.rate) AS total_bet, SUM(total_win/fx_rate.rate) AS total_win, 
	SUM(total_bet_count) AS total_round, SUM(total_bet/fx_rate.rate) AS valid_bet,
	SUM(total_bet/fx_rate.rate)-SUM(total_win/fx_rate.rate) AS income, user_list.currency,
	0 AS total_rake, 0 AS room_fee, game_info.onlinetime, game_info.brand, pf.firstGamingTime,
	IF(pf.firstGamingTime>=@date1 AND pf.firstGamingTime<DATE_ADD(@date1,INTERVAL 1 DAY),1,0) AS `(non)first`
FROM
cypress.statistic_user_by_lottogame AS stat
JOIN MaReport.game_info ON game_info.gid = stat.gid
JOIN cypress.user_list ON user_list.id = stat.uid
JOIN cypress.parent_list ON parent_list.id = user_list.parentid
JOIN cypress.parent_list AS owner_list ON owner_list.id = user_list.ownerid
JOIN cypress.fx_rate ON user_list.currency=fx_rate.short_name
JOIN dataWarehouse.player_firstTime_lastTime_gaming_info AS pf ON owner_list.id=pf.oid AND parent_list.id=pf.pid AND user_list.id=pf.uid AND pf.gid=game_info.gid
WHERE `date`>=@date1 AND `date`<DATE_ADD(@date1, INTERVAL 1 DAY) AND parent_list.istestss=0 AND owner_list.istestss=0 AND stat.gid=869
GROUP BY stat.gid, user_list.ownerid, user_list.parentid, stat.uid;

-- 
SET @date1 = '2022-09-30';
SELECT players_first FROM MaReport.report_by_game_daily AS rgd WHERE `date`>= @date1 AND `date`<DATE_ADD(@date1, INTERVAL 1 DAY) AND currency='ALL' AND gid =1
ORDER BY bets DESC
LIMIT 500;
-- JOIN MaReport.game_info ON rgd.gid = game_info.gid LIMIT 300;

SELECT * FROM MaReport.report_by_game_monthly AS rep
JOIN MaReport.game_info ON  game_info.gid=rep.gid
WHERE `date` = @date1 AND currency = 'ALL' 
ORDER BY bets DESC
LIMIT 500;

SELECT * FROM cypress.statistic_user_by_game WHERE `date` >= @date1 AND `date`<DATE_ADD(@date1, INTERVAL 1 DAY) LIMIT 1000000;

SELECT * FROM cypress.fx_rate;

SELECT * FROM MaReport.report_daily_user LIMIT 300;
-- 
SELECT @date1;
SELECT
                SUM(total_bet)/rate, SUM(total_round)
            FROM
            cypress.statistic_user_by_game AS stat
            JOIN MaReport.game_info ON game_info.gid = stat.gid
            JOIN cypress.user_list ON user_list.id = stat.uid
            JOIN cypress.parent_list ON parent_list.id = user_list.parentid
            JOIN cypress.parent_list AS owner_list ON owner_list.id = user_list.ownerid
            JOIN cypress.fx_rate ON user_list.currency=fx_rate.short_name
            WHERE `date`>=@date1 AND `date`<DATE_ADD(@date1, INTERVAL 1 DAY) AND parent_list.istestss=0 AND owner_list.istestss=0 AND stat.gid=1
            GROUP BY stat.gid-- , user_list.ownerid, user_list.parentid, stat.uid
            ;
            
-- 
SELECT game_type FROM dataWarehouse.player_game_by_hour AS pg JOIN MaReport.game_info ON pg.gid=game_info.gid WHERE game_type = 'lotto';
SELECT game_type FROM dataWarehouse.player_fish_game_by_hour AS pg JOIN MaReport.game_info ON pg.gid=game_info.gid;
SELECT game_type FROM dataWarehouse.player_table_game_by_hour AS pg JOIN MaReport.game_info ON pg.gid=game_info.gid;

SELECT * FROM dataWarehouse.player_by_day;
SELECT * FROM MaReport.user_gametoken_log
WHERE `date`>=@date1 AND `date`<DATE_ADD(@date1, INTERVAL 1 DAY);

SELECT 
	user_list.ownerid AS oid, user_list.parentid AS pid, user_list.id, user_list.`account`, game_code
FROM  cypress.user_list 
JOIN cypress.parent_list ON user_list.parentid=parent_list.id
JOIN cypress.parent_list AS owner_list ON user_list.ownerid=owner_list.id
JOIN 
(
	SELECT uid, game_code FROM
	(
		SELECT uid, stat.gid, game_code FROM cypress.statistic_user_by_game AS stat
        JOIN MaReport.game_info ON game_info.gid = stat.gid
		WHERE `date`>@date1 AND `date`<DATE_ADD(@date1, INTERVAL 1 DAY)
		UNION
		SELECT uid, stat.gid, game_code FROM cypress.statistic_user_by_lottogame AS stat
        JOIN MaReport.game_info ON game_info.gid = stat.gid
		WHERE `date`>@date1 AND `date`<DATE_ADD(@date1, INTERVAL 1 DAY)
		UNION
		SELECT uid, stat.gid, game_code FROM cypress.statistic_user_by_tablegame AS stat
        JOIN MaReport.game_info ON game_info.gid = stat.gid
		WHERE `date`>@date1 AND `date`<DATE_ADD(@date1, INTERVAL 1 DAY)
	) AS tb
	GROUP BY uid, gid
) AS stat ON stat.uid=user_list.id
WHERE owner_list.istestss=0 AND parent_list.istestss=0 AND user_list.id=169066804 LIMIT 10000;

SELECT * FROM dataWarehouse.player_firstTime_lastTime_gaming_info;
SELECT * FROM MaReport.user_gametoken_log;

SELECT
	stat.gid, user_list.ownerid AS oid, user_list.parentid AS pid, stat.uid, user_list.`account`, player_first.firstGamingTime
FROM
cypress.statistic_user_by_game AS stat
JOIN MaReport.game_info ON game_info.gid = stat.gid
JOIN cypress.user_list ON user_list.id = stat.uid
JOIN cypress.parent_list ON parent_list.id = user_list.parentid
JOIN cypress.parent_list AS owner_list ON owner_list.id = user_list.ownerid
JOIN cypress.fx_rate ON user_list.currency=fx_rate.short_name
JOIN dataWarehouse.player_firstTime_lastTime_gaming_info AS player_first ON player_first.uid = stat.uid 
WHERE `date`>=@date1 AND `date`<DATE_ADD(@date1, INTERVAL 1 DAY) AND parent_list.istestss=0 AND owner_list.istestss=0 AND stat.gid=1
GROUP BY stat.gid, user_list.ownerid, user_list.parentid, stat.uid;

SELECT 
	owner_list.id AS oid, parent_list.id AS pid, user_list.id, 
	ugl.game_code, MIN(starttime), MAX(endtime), pf.firstGamingTime,
    IF(pf.firstGamingTime>=@date1 AND pf.firstGamingTime<DATE_ADD(@date1,INTERVAL 1 DAY),1,0), SUM(total_bet)
FROM 
MaReport.user_gametoken_log AS ugl
JOIN cypress.user_list ON user_list.userid=ugl.userid
JOIN cypress.parent_list ON user_list.parentid = parent_list.id
JOIN cypress.parent_list AS owner_list ON owner_list.id = user_list.ownerid
JOIN cypress.statistic_user_by_game AS stat ON stat.uid = user_list.id
JOIN MaReport.game_info ON game_info.game_code=ugl.game_code
JOIN dataWarehouse.player_firstTime_lastTime_gaming_info AS pf ON owner_list.id=pf.oid AND parent_list.id=pf.pid AND user_list.id=pf.uid AND pf.gid=game_info.gid
WHERE ugl.`date`>=DATE_ADD(@date1, INTERVAL 23 HOUR) AND ugl.`date`<DATE_ADD(@date1,INTERVAL 1 DAY) AND ugl.game_code = 1
GROUP BY owner_list.id, parent_list.id, user_list.id;

SELECT * FROM MaReport.user_gametoken_log;

SELECT 
	owner_list.id AS oid, parent_list.id AS pid, user_list.id, 
	ugl.game_code, IF(pf.firstGamingTime>=@date1 AND pf.firstGamingTime<DATE_ADD(@date1,INTERVAL 1 DAY),1,0),
	MIN(starttime), MAX(endtime)
FROM 
MaReport.user_gametoken_log AS ugl
JOIN cypress.user_list ON user_list.userid=ugl.userid
JOIN cypress.parent_list ON user_list.parentid = parent_list.id
JOIN cypress.parent_list AS owner_list ON owner_list.id = user_list.ownerid
JOIN MaReport.game_info ON game_info.game_code=ugl.game_code
JOIN dataWarehouse.player_firstTime_lastTime_gaming_info AS pf ON owner_list.id=pf.oid AND parent_list.id=pf.pid AND user_list.id=pf.uid AND pf.gid=game_info.gid
WHERE `date`>=@date1 AND `date`<DATE_ADD(@date1,INTERVAL 1 DAY) AND ugl.game_code = 'AB1'
GROUP BY owner_list.id, parent_list.id, user_list.id;


SELECT -- SUM(plat_web), SUM(plat_mobile), SUM(plat_pc), SUM(plat_mobile)/(SUM(plat_web)+SUM(plat_mobile)+SUM(plat_pc)) 
	*
FROM cypress.statistic_game
WHERE `date`>=@date1 AND `date`<DATE_ADD(@date1, INTERVAL 1 DAY) AND gid = 1; -- for h5_rate maybe can??

SELECT SUM(h5_round)/SUM(total_round) 
FROM MaReport.report_rounds_daily WHERE `date`>=@date1 AND `date`<DATE_ADD(@date1, INTERVAL 1 DAY) AND gid = 1;

SHOW variables like "%char%";
SHOW variables like "%table_size%";

SELECT 
	user_list.id, 
	ugl.game_code,-- , IF(pf.firstGamingTime>=@date1 AND pf.firstGamingTime<DATE_ADD(@date1,INTERVAL 1 DAY),1,0),
	MIN(starttime), MAX(endtime)-- , SUM(total_bet)
FROM 
MaReport.user_gametoken_log AS ugl
JOIN cypress.user_list ON user_list.userid=ugl.userid
JOIN cypress.parent_list ON user_list.parentid = parent_list.id
JOIN cypress.parent_list AS owner_list ON owner_list.id = user_list.ownerid
-- JOIN cypress.statistic_user_by_game AS stat ON stat.uid = user_list.id
JOIN MaReport.game_info ON game_info.game_code=ugl.game_code
JOIN dataWarehouse.player_firstTime_lastTime_gaming_info AS pf ON owner_list.id=pf.oid AND parent_list.id=pf.pid AND user_list.id=pf.uid AND pf.gid=game_info.gid
WHERE ugl.`date`>=@date1 AND ugl.`date`<DATE_ADD(@date1,INTERVAL 1 DAY) AND game_info.game_code = 7
GROUP BY user_list.id;

SET @date1 = '2022-09-30';

SELECT 
	owner_list.id AS oid, parent_list.id AS pid, user_list.id, 
	ugl.game_code, -- IF(pf.firstGamingTime>=@date1 AND pf.firstGamingTime<DATE_ADD(@date1,INTERVAL 1 DAY),1,0),
	MIN(starttime), MAX(endtime)-- , SUM(total_bet)
FROM 
MaReport.user_gametoken_log AS ugl
JOIN cypress.user_list ON user_list.userid=ugl.userid
JOIN cypress.parent_list ON user_list.parentid = parent_list.id
JOIN cypress.parent_list AS owner_list ON owner_list.id = user_list.ownerid
-- JOIN cypress.statistic_user_by_tablegame AS stat ON stat.uid = user_list.id
JOIN MaReport.game_info ON game_info.game_code=ugl.game_code
-- JOIN dataWarehouse.player_firstTime_lastTime_gaming_info AS pf ON owner_list.id=pf.oid AND parent_list.id=pf.pid AND user_list.id=pf.uid AND pf.gid=game_info.gid
WHERE ugl.`date`>=@date1 AND ugl.`date`<DATE_ADD(@date1,INTERVAL 1 DAY) AND ugl.game_code = 7
GROUP BY owner_list.id, parent_list.id, user_list.id;

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
) AS tb
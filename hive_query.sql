create table top_rus_hadoop as
with HADOOP_QUEST as (
    select Id, Tags from Posts
    where PostTypeId = 1 and Tags like '%hadoop%'
    ),
    HADOOP_ANSWERS as (
      select P.Score, P.OwnerUserId, HQ.Tags
      from Posts P inner join HADOOP_QUEST HQ on (HQ.Id = P.ParentId)
    ),
    RU_USERS as (
      select Id, Location, DisplayName, AccountId
      from Users U
      where lower(U.Location) like '%russia%'
        or lower(U.Location) like '%russian federation%'
        or lower(U.Location) like '%moscow%'
        or lower(U.Location) like '%saint petersburg%'
        or lower(U.Location) like '%россия%' 
        or lower(U.Location) like '%москва%'
        or lower(U.Location) like '%санкт-петерург%'
    )
    select RU.Location, 
           RU.DisplayName, 
           RU.AccountId, 
           SUM(HA.Score) as Sum_scr,
           count(*) as answered_num
    from RU_USERS RU inner join HADOOP_ANSWERS HA on (RU.id = HA.OwnerUserId)
    group by RU.Location, RU.DisplayName, RU.AccountId
    sort by Sum_scr desc
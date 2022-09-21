'BountyStart', 0.0, 21)
('BountyStart', 1.0, 72)
('BountyStart', 2.0, 28)
('BountyStart', 3.0, 56)
('BountyStart', 4.0, 36)
('BountyStart', 5.0, 50)
('BountyStart', 6.0, 30)
('Favorite', 0.0, 894)
('Favorite', 1.0, 1839)
('Favorite', 2.0, 2045)
('Favorite', 3.0, 2173)
('Favorite', 4.0, 2159)
('Favorite', 5.0, 1889)
('Favorite', 6.0, 857)


# combined
"""
with v as (select postid, count(postid) as vote_count
from votes
group by postid),
w as (select posts.parentid as family, sum(v.vote_count) as total
from posts, v
where v.postid = posts.parentid
group by posts.parentid)

select posts.id, posts.title
from posts, w, v
where posts.id= w.family and v.vote_count + w.total >= 100
group by posts.id
order by posts.id asc;
"""

CREATE TABLE Users (
	Id integer NOT NULL PRIMARY KEY,
	Reputation integer NOT NULL,
	CreationDate date NOT NULL,
	DisplayName varchar(40) NULL,
	Views integer NOT NULL,
	UpVotes integer NOT NULL,
	DownVotes integer NOT NULL
);

CREATE TABLE PostTypes (
	PostTypeId integer NOT NULL PRIMARY KEY,
	Description varchar(250) NULL
);

insert into PostTypes values 
	(1, 'Question'),
	(2, 'Answer'),
	(3, 'Orphaned tag wiki'),
	(4, 'Tag wiki excerpt'),
	(5, 'Tag wiki'),
	(6, 'Moderator nomination'),
	(7, 'Wiki placeholder'),
	(8, 'Privilege wiki');

CREATE TABLE Posts (
	Id integer NOT NULL PRIMARY KEY,
	PostTypeId integer NOT NULL references PostTypes,
	Title varchar(250) NULL,
	AcceptedAnswerId integer NULL,
	ParentId integer NULL references Posts(Id),
	CreationDate date NOT NULL,
	Score integer NOT NULL,
	ViewCount integer NULL,
	OwnerUserId integer NOT NULL references Users(Id),
	LastEditorUserId integer NOT NULL references Users(Id),
	Tags varchar(250) NULL
);             

CREATE TABLE Badges (
	Id integer NOT NULL Primary key,
	UserId integer NOT NULL references Users(Id),
	Name varchar(50) NOT NULL,
	Date date NOT NULL,
	Class integer NOT NULL
);

CREATE TABLE Comments (
	Id integer NOT NULL PRIMARY KEY,
	Text varchar(600) NOT NULL,
	PostId integer NOT NULL references posts(Id),
	Score integer NOT NULL,
	CreationDate date NOT NULL,
	UserId integer NOT NULL references Users(Id)
);

CREATE TABLE VoteTypes (
	VoteTypeId integer NOT NULL PRIMARY KEY,
	Description varchar(100)
);

CREATE TABLE Votes (
	Id integer PRIMARY KEY,
	PostId integer NOT NULL references posts(Id),
	VoteTypeId integer NOT NULL references VoteTypes(VoteTypeId),
	UserId integer NOT NULL references Users(Id),
	CreationDate date NULL,
	BountyAmount integer NULL
);

insert into VoteTypes values 
(1, 'AcceptedByOriginator'),
(2, 'UpMod (AKA upvote)'),
(3,  'DownMod (AKA downvote)'),
(4, 'Offensive'),
(5, 'Favorite'),
(6, 'Close'),
(7, 'Reopen'),
(8, 'BountyStart'),
(9, 'BountyClose'),
(10, 'Deletion'),
(11, 'Undeletion'),
(12, 'Spam'),
(15, 'ModeratorReview'),
(16, 'ApproveEditSuggestion');
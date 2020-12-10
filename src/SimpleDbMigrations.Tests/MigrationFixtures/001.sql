CREATE TABLE [Fruit](
	[Id] uniqueidentifier NOT NULL,
	[Name] nvarchar(255) NOT NULL,
	[Color] nvarchar(255) NOT NULL,
	[Created] datetimeoffset NOT NULL
	CONSTRAINT [PK_Fruit] PRIMARY KEY CLUSTERED ([Id] ASC)
)
GO

ALTER TABLE [Fruit] ADD CONSTRAINT [DF_Fruit_Id]  DEFAULT (newsequentialid()) FOR [Id]
GO

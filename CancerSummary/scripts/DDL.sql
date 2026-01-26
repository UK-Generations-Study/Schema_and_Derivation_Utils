SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[NewCancerSummary](
	[STUDY_ID] [int] NOT NULL,
	[S_STUDY_ID] [nvarchar](100) NOT NULL,
	[TUMOUR_ID] [bigint] NULL,
	[S_TUMOUR_ID] [nvarchar](100) NULL,
	[DIAGNOSIS_DATE] [datetime] NULL,
	[S_DIAGNOSIS_DATE] [nvarchar](100) NULL,
	[AGE_AT_DIAGNOSIS] [int] NULL,
	[ICD_CODE] [nvarchar](100) NULL,
	[S_ICD_CODE] [nvarchar](100) NULL,
	[MORPH_CODE] [int] NULL,
	[S_MORPH_CODE] [nvarchar](100) NULL,
	[CANCER_SITE] [nvarchar](100) NULL,
	[GRADE] [nvarchar](100) NULL,
	[S_GRADE] [nvarchar](100) NULL,
	[TUMOUR_SIZE] [float] NULL,
	[S_TUMOUR_SIZE] [nvarchar](100) NULL,
	[NODES_TOTAL] [int] NULL,
	[S_NODES_TOTAL] [nvarchar](100) NULL,
	[NODES_POSITIVE] [int] NULL,
	[S_NODES_POSITIVE] [nvarchar](100) NULL,
	[STAGE] [nvarchar](100) NULL,
	[S_STAGE] [nvarchar](100) NULL,
	[ER_STATUS] [nvarchar](100) NULL,
	[S_ER_STATUS] [nvarchar](100) NULL,
	[PR_STATUS] [nvarchar](100) NULL,
	[S_PR_STATUS] [nvarchar](100) NULL,
	[HER2_STATUS] [nvarchar](100) NULL,
	[S_HER2_STATUS] [nvarchar](100) NULL,
	[HER2_FISH] [nvarchar](100) NULL,
	[Ki67] [float] NULL,
	[SCREEN_DETECTED] [nvarchar](100) NULL,
	[S_SCREEN_DETECTED] [nvarchar](100) NULL,
	[SCREENINGSTATUSCOSD_CODE] [nvarchar](100) NULL,
	[LATERALITY] [nvarchar](100) NULL,
	[S_LATERALITY] [nvarchar](100) NULL,
	[T_STAGE] [nvarchar](100) NULL,
	[S_T_STAGE] [nvarchar](100) NULL,
	[N_STAGE] [nvarchar](100) NULL,
	[S_N_STAGE] [nvarchar](100) NULL,
	[M_STAGE] [varchar](100) NULL,
	[S_M_STAGE] [varchar](100) NULL,
	[CREATED_TIME] [datetime] NULL,
	[COMMENTS] [nvarchar](100) NULL,
	[SUMMARY_ID] [int] IDENTITY(1,1) NOT NULL,
	[GROUPED_SITE] [nvarchar](100) NULL,
PRIMARY KEY CLUSTERED 
(
	[SUMMARY_ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO
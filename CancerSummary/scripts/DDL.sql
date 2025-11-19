SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[NewCancerSummary](
	[SUMMARY_ID] [int] IDENTITY(1,1) NOT NULL,
	[STUDY_ID] [int] NOT NULL,
	[S_STUDY_ID] [int] NOT NULL,
	[TUMOUR_ID] [int] NULL,
	[S_TUMOUR_ID] [varchar](50) NULL,
	[DIAGNOSIS_DATE] [datetime] NULL,
	[S_DIAGNOSIS_DATE] [varchar](50) NULL,
	[AGE_AT_DIAGNOSIS] [int] NULL,
	[ICD_CODE] [varchar](50) NULL,
	[S_ICD_CODE] [varchar](50) NULL,
	[MORPH_CODE] [int] NULL,
	[S_MORPH_CODE] [varchar](50) NULL,
	[CANCER_SITE] [varchar](50) NULL,
	[GRADE] [varchar](50) NULL,
	[S_GRADE] [varchar](50) NULL,
	[TUMOUR_SIZE] [float] NULL,
	[S_TUMOUR_SIZE] [varchar](50) NULL,
	[NODES_TOTAL] [int] NULL,
	[S_NODES_TOTAL] [varchar](50) NULL,
	[NODES_POSITIVE] [int] NULL,
	[S_NODES_POSITIVE] [varchar](50) NULL,
	[STAGE] [varchar](50) NULL,
	[S_STAGE] [varchar](50) NULL,
	[ER_STATUS] [varchar](50) NULL,
	[S_ER_STATUS] [varchar](50) NULL,
	[PR_STATUS] [varchar](50) NULL,
	[S_PR_STATUS] [varchar](50) NULL,
	[HER2_STATUS] [varchar](50) NULL,
	[S_HER2_STATUS] [varchar](50) NULL,
	[HER2_FISH] [varchar](50) NULL,
	[Ki67] [float] NULL,
	[SCREEN_DETECTED] [varchar](50) NULL,
	[S_SCREEN_DETECTED] [varchar](50) NULL,
	[SCREENINGSTATUSCOSD_CODE] [varchar](50) NULL,
	[LATERALITY] [varchar](50) NULL,
	[S_LATERALITY] [varchar](50) NULL,
	[T_STAGE] [varchar](50) NULL,
	[S_T_STAGE] [varchar](50) NULL,
	[N_STAGE] [varchar](50) NULL,
	[S_N_STAGE] [varchar](50) NULL,
	[M_STAGE] [varchar](50) NULL,
	[S_M_STAGE] [varchar](50) NULL,
	[CREATED_TIME] [datetime] NULL,
	[COMMENTS] [varchar](50) NULL
PRIMARY KEY CLUSTERED 
(
	[SUMMARY_ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO
-- Copy data from sub.txt
COPY INTO SUB 
FROM @s3_stage/:period_folder/
FILES = ('sub.txt') 
FILE_FORMAT = TXT_FILE_FORMAT
CREDENTIALS = (
    AWS_KEY_ID = :aws_key_id
    AWS_SECRET_KEY = :aws_secret_key
)
ON_ERROR = 'ABORT_STATEMENT';

-- Copy data from num.txt
COPY INTO NUM 
FROM @s3_stage/:period_folder/
FILES = ('num.txt') 
FILE_FORMAT = TXT_FILE_FORMAT
CREDENTIALS = (
    AWS_KEY_ID = :aws_key_id
    AWS_SECRET_KEY = :aws_secret_key
)
ON_ERROR = 'ABORT_STATEMENT';

-- Copy data from tag.txt
COPY INTO TAG 
FROM @s3_stage/:period_folder/
FILES = ('tag.txt') 
FILE_FORMAT = TXT_FILE_FORMAT
CREDENTIALS = (
    AWS_KEY_ID = :aws_key_id
    AWS_SECRET_KEY = :aws_secret_key
)
ON_ERROR = 'ABORT_STATEMENT';

-- Copy data from pre.txt
COPY INTO PRE 
FROM @s3_stage/:period_folder/
FILES = ('pre.txt') 
FILE_FORMAT = TXT_FILE_FORMAT
CREDENTIALS = (
    AWS_KEY_ID = :aws_key_id
    AWS_SECRET_KEY = :aws_secret_key
)
ON_ERROR = 'ABORT_STATEMENT';
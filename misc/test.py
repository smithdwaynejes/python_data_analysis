""" # my routine to extract all files of csv.
def listofFiles(dirPath, extension=""):
    if not extension:
        return os.listdir(dirPath)

    return [file for file in os.listdir(dirPath) if file.endswith("." + extension)]

# setting up directory path
dirpath = '/home/user/fund/{}/'.format(arguments[0])

# List out csv file names from the directory to process
# my routine to extract all files of csv.
list_of_files = listofFiles(dirpath, "csv")

# Store date of the filename into a variable
if list_of_files:
    date = list_of_files[0].rsplit('.')[0].rsplit('_')[0]

# Extract times from the files and make list of times and sort it
time_list = set()
for path in list_of_files:
    if path:
        if "_" in path:
            time = path.rsplit('.')[0].rsplit('_')[1]
            time_list.add(dt.strptime(time, "%H:%M:%S"))

time_list = sorted(time_list)

# Get earliest file and load into pandas Data Frame
time_s = dt.strftime(time_list[0], "%H:%M:%S")
file = "{}_{}.csv".format(date, time_s)
merged_df = pd.read_csv(dirpath + file)
# Filter only needed column
merged_df = start_df = merged_df[['Scheme Name', 'pri']]
# here merged_df for generating resulting data frame
# start_df for comparing data of new one with earliest data frame

# Rename the name of the column 'pri' with 'pri_[time_of_the_file]'

start_suffix = dt.strftime(time_list[1], "_%H:%M")
merged_df = merged_df.rename(columns={'pri': 'pri{}'.format(start_suffix)})

# Start Iterating with next time file
for time in time_list[1:]:

    time_s = dt.strftime(time, "%H:%M:%S")  # for making filename
    # for making columns as per filename
    end_prefix = dt.strftime(time, "_%H:%M")
    file = "{}_{}.csv".format(date, time_s)  # Set file name
    frame = pd.read_csv(dirpath + file)  # Read csv

    frame = frame[['Scheme Name', 'pri']]

    # prepare Intersected list with previous time file
    inter_df = pd.merge(start_df, frame, on='Scheme Name', how='inner',
                        suffixes=[start_suffix, end_prefix])

    # Append the current time price column for resulting data frame
    merged_df = pd.merge(merged_df, inter_df[[
                            'Scheme Name', 'pri'+end_prefix]], on='Scheme Name', how='right')

    start_df = frame  # Make the current data frame as previous
    start_suffix = end_prefix  # Change the previous time suffix to current

# print the result
print(merged_df.head())

# Check the pair of price columns from earliest to newest If there is a price change for the funds.
start = dt.strftime(time_list[0], "%H:%M")
for time in time_list[1:]:
    end = dt.strftime(time, "%H:%M")
    print("Comparing prices consistency between {} and {}".format(start, end))
    print(merged_df.loc[merged_df['pri_'+start]
                        != merged_df['pri_'+end]].dropna())
    print("---------------------------------------------------------------------")
    start = end
 """
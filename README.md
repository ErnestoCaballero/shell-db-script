## Java Program Data Fix Script

This script was developed to address issues arising from incomplete data generated by a Java program, causing disruptions in downstream processes. It employs a combination of shell commands to rectify the situation.

### Tools Used:
- **Shell Scripting**: Utilized shell scripting with commands such as `find` for file searching, `gawk` for content processing, and `sqlplus` for database interaction.
- **SQL**: Leveraged SQL to create tables, populate them with missing data, and facilitate data transport between databases using db links.
- **File Formatting**: Employed commands like `SPOOL` and `sed` for file formatting and to dispatch notifications regarding unresolved cases to the team.

### Functionality:
- **File Search**: The script utilizes `find` command to locate files generated by the Java program.
- **Data Processing**: It employs `gawk` for processing the content of these files.
- **Database Interaction**: Utilizes `sqlplus` to interact with databases, create tables, fill missing data, and transport data between databases.
- **File Formatting**: Commands like `SPOOL` and `sed` are used for formatting files.
- **Notification**: Sends notifications to the team regarding unresolved cases.

### Usage:
1. Ensure necessary permissions are granted to execute the script.
2. Execute the script providing required parameters.

### Example Usage:
```bash
$ ./data_fix_script.sh process_no

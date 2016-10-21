Importing APEX Code Style in IntelliJ
=====================================

With IntelliJ you can either import settings globally for all the workspaces or apply the style to a specific project. These settings are also set automatically if the project is imported into IntelliJ directly using `Import Project`.

##Import settings globally
To share settings between all the workspaces you can import the `.settings/apex-style.jar` which contains the apex-style. 

####To import the settings from the jar archive
1. On the main menu, choose File | Import Settings.
2. In the Import File Location dialog box that opens select the `.settings/apex-style.jar`.
3. The Select Components to Import dialog box that opens next will only have an entry for code style which by default is selected. Click on OK. 
4. Restart IntelliJ once the jar is successfully imported.

####To apply APEX code style
1. On the main menu, choose IntelliJ | Preferences.
2. Go to Editor | Code Style.
3. In the 'Scheme' drop down select 'apex-style'.

##Apply settings to a specific project
1. There is a .idea folder under the Apex project which contains a codeStyleSettings.xml that describes APEX style.
2. On the main menu of IntelliJ, choose IntelliJ | Preferences.
3. Go to Editor | Code Style.
4. In the 'Scheme' drop down select 'Project'.

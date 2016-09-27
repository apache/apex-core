Importing APEX Code Style in Eclipse
====================================
For Eclipse there are 2 configuration files
1. .settings/apex-style.xml : this contains Java formatter settings.
2. .settings/apex-importorder.importorder : this specifies the accepted import order for Apex projects.

##To import Apex format style
1. On the main menu, choose Eclipse | Preferences.
2. Go to Java | Code Style | Formatter and click on 'Import'.
3. In the Import Profile dialog box that opens select the `.settings/apex-style.xml`.
4. Apply the changes.

##To import Apex import order
1. On the main menu, choose Eclipse | Preferences.
2. Go to Java | Code Style | Organize Imports and click on 'Import'.
3. In the Load Import Order from File dialog box that opens select `.settings/apex-importorder.importorder`.
4. Apply the changes.

##Trailing whitespace
Apex checkstyle enforces no trailing whitespace. You can have Eclipse automatically remove trailing whitespace:
http://stackoverflow.com/questions/1043433/how-to-auto-remove-trailing-whitespace-in-eclipse/2618521#2618521

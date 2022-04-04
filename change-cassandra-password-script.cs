using Skyline.DataMiner.Net.Messages.SLDataGateway;
using Skyline.DataMiner.Net.Messages;
using Skyline.DataMiner.Automation;
using Skyline.DataMiner.Net;
using System;

public class Script
{
    public void Run(IEngine engine)
    {
        // Script can only run when the Cassandra database is active
        if (!IsCassandra(engine))
        {
            engine.ExitFail("This DataMiner system is not using the Cassandra database. Cannot execute.");
            return;
        }

        // Create the dialog box
        UIBuilder uib = new UIBuilder();

        // Configure the dialog box
        uib.RequireResponse = true;
        uib.RowDefs = "a;a;a;a;a;a";
        uib.ColumnDefs = "a;a";

        UIBlockDefinition pwdText = new UIBlockDefinition();
        pwdText.Type = UIBlockType.StaticText;
        pwdText.Text = "Enter the new Cassandra password:";
        pwdText.Height = 20;
        pwdText.Row = 0;
        pwdText.Column = 0;
        uib.AppendBlock(pwdText);

        UIBlockDefinition pwd = new UIBlockDefinition();
        pwd.Type = UIBlockType.PasswordBox;
        pwd.Height = 25;
        pwd.Width = 150;
        pwd.Row = 1;
        pwd.Column = 0;
        pwd.WantsOnChange = true;
        pwd.DestVar = nameof(pwd);
        pwd.IsRequired = true;
        pwd.ValidationState = UIValidationState.Invalid;
        pwd.ValidationText = "Passwords do not match 1";
        uib.AppendBlock(pwd);

        UIBlockDefinition repeatPwdText = new UIBlockDefinition();
        repeatPwdText.Type = UIBlockType.StaticText;
        repeatPwdText.Text = "Repeat the new Cassandra password:";
        repeatPwdText.Height = 20;
        repeatPwdText.Row = 2;
        repeatPwdText.Column = 0;
        uib.AppendBlock(repeatPwdText);

        UIBlockDefinition pwdRepeat = new UIBlockDefinition();
        pwdRepeat.Type = UIBlockType.PasswordBox;
        pwdRepeat.Height = 25;
        pwdRepeat.Width = 150;
        pwdRepeat.Row = 3;
        pwdRepeat.Column = 0;
        pwdRepeat.ValidationText = "Passwords do not match";
        pwdRepeat.ValidationState = UIValidationState.Invalid;
        pwdRepeat.WantsOnChange = true;
        pwdRepeat.DestVar = nameof(pwdRepeat);
        pwdRepeat.IsRequired = true;
        uib.AppendBlock(pwdRepeat);

        UIBlockDefinition changePwdButton = new UIBlockDefinition();
        changePwdButton.Type = UIBlockType.Button;
        changePwdButton.Text = "Change Password";
        changePwdButton.Height = 20;
        changePwdButton.Width = 150;
        changePwdButton.Row = 4;
        changePwdButton.Column = 0;
        changePwdButton.DestVar = nameof(changePwdButton);
        uib.AppendBlock(changePwdButton);

        UIBlockDefinition validationState = new UIBlockDefinition();
        validationState.Type = UIBlockType.StaticText;
        validationState.Text = "Validation State is: ";
        validationState.Height = 20;
        validationState.Row = 5;
        validationState.Column = 0;
        uib.AppendBlock(validationState);

        bool changePassword = false;

        UIResults uir = null;
        bool isValid = false;
        string newPassword = "";

        do
        {
            // Display the dialog box
            uir = engine.ShowUI(uib);

            if (uir == null)
            {
                isValid = false;
                continue;
            }

            newPassword = uir.GetString(nameof(pwd));
            string pwdRepeatValue = uir.GetString(nameof(pwdRepeat));

            changePassword = uir.WasButtonPressed(nameof(changePwdButton));

            pwdRepeat.InitialValue = pwdRepeatValue;
            pwd.InitialValue = newPassword;

            isValid = !string.IsNullOrWhiteSpace(newPassword) && newPassword.Length > 6 && newPassword == pwdRepeatValue;

            pwd.ValidationState = isValid ? UIValidationState.Valid : UIValidationState.Invalid;
            pwdRepeat.ValidationState = isValid ? UIValidationState.Valid : UIValidationState.Invalid;

            validationState.Text = "Validation State is: " + pwdRepeat.ValidationState;
        }
        while (!isValid || !changePassword);

        ChangeCassandraPassword(engine, newPassword);

        engine.ExitSuccess("Cassandra database password was changed");
    }

    private bool IsCassandra(IEngine engine)
    {
        var dbInfo = GetDbInfo(engine);
        var dbType = dbInfo.LocalDatabaseInfo.DatabaseType;

        return dbType == DBMSType.Cassandra || dbType == DBMSType.CassandraCluster;
    }

    private GetDataBaseInfoResponseMessage GetDbInfo(IEngine engine)
    {
        return engine.SendSLNetSingleResponseMessage(new GetInfoMessage(InfoType.Database)) as GetDataBaseInfoResponseMessage;
    }

    private void ChangeCassandraPassword(IEngine engine, string newPassword)
    {
        var dbInfo = GetDbInfo(engine);
        string dbUser = dbInfo.LocalDatabaseInfo.User;

        engine.GenerateInformation($"{engine.UserDisplayName} is changing the password for the '{dbUser}' Cassandra role");
        string newRoleQuery = $"ALTER ROLE {dbUser} WITH PASSWORD='{newPassword}'";

        Query(engine, newRoleQuery, newPassword);
        UpdateDataMinerDatabasePassword(engine, newPassword);
    }

    private void Query(IEngine engine, string query, string newPassword)
    {
        var request = new ExecuteDatabaseQueryRequest()
        {
            Query = query
        };

        var response = engine.SendSLNetSingleResponseMessage(request) as ExecuteDatabaseQueryResponse;
        bool success = response.Result?.Length == 0;

        if (!success)
        {
            string errorMessage = "Failed to change the password: " + string.Join(Environment.NewLine, response?.Result[0]);

            //Make sure we don't log the new password
            errorMessage = errorMessage.Replace(newPassword, "***********");
            engine.ExitFail(errorMessage);
        }
    }

    private void UpdateDataMinerDatabasePassword(IEngine engine, string password)
    {
        engine.GenerateInformation("Updating DataMiner database password");

        engine.SendSLNetSingleResponseMessage(new UpdateDatabaseSettingsMessage()
        {
            DatabaseConfigType = DatabaseType.LocalDatabase,
            ChangedFields = DatabaseSettingsChangedFields.PWD,
            PWD = password,
        });
    }
}
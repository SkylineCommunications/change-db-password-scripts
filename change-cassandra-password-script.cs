using Skyline.DataMiner.Net.Messages.SLDataGateway;
using System.Security.Authentication;
using Skyline.DataMiner.Net.Messages;
using Skyline.DataMiner.Automation;
using Skyline.DataMiner.Net;
using Cassandra;
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
        uib.RowDefs = "a;a;a;a;a;a;a;a;a;a";
        uib.ColumnDefs = "a;a";

        UIBlockDefinition userLabel = new UIBlockDefinition();
        userLabel.Type = UIBlockType.StaticText;
        userLabel.Text = "Cassandra username:";
        userLabel.Height = 20;
        userLabel.Row = 0;
        userLabel.Column = 0;
        uib.AppendBlock(userLabel);

        UIBlockDefinition userText = new UIBlockDefinition();
        userText.Type = UIBlockType.TextBox;
        userText.Height = 25;
        userText.Width = 150;
        userText.Row = 1;
        userText.Column = 0;
        userText.DestVar = nameof(userText);
        userText.IsRequired = true;
        userText.InitialValue = GetCassandraUser(engine);
        uib.AppendBlock(userText);

        UIBlockDefinition currentPwdLabel = new UIBlockDefinition();
        currentPwdLabel.Type = UIBlockType.StaticText;
        currentPwdLabel.Text = "Enter the current Cassandra password:";
        currentPwdLabel.Height = 20;
        currentPwdLabel.Row = 2;
        currentPwdLabel.Column = 0;
        uib.AppendBlock(currentPwdLabel);

        UIBlockDefinition currentPwd = new UIBlockDefinition();
        currentPwd.Type = UIBlockType.PasswordBox;
        currentPwd.Height = 25;
        currentPwd.Width = 150;
        currentPwd.Row = 3;
        currentPwd.Column = 0;
        currentPwd.DestVar = nameof(currentPwd);
        currentPwd.IsRequired = true;
        uib.AppendBlock(currentPwd);

        UIBlockDefinition pwdText = new UIBlockDefinition();
        pwdText.Type = UIBlockType.StaticText;
        pwdText.Text = "Enter the new Cassandra password:";
        pwdText.Height = 20;
        pwdText.Row = 4;
        pwdText.Column = 0;
        uib.AppendBlock(pwdText);

        UIBlockDefinition pwd = new UIBlockDefinition();
        pwd.Type = UIBlockType.PasswordBox;
        pwd.Height = 25;
        pwd.Width = 150;
        pwd.Row = 5;
        pwd.Column = 0;
        pwd.WantsOnChange = true;
        pwd.DestVar = nameof(pwd);
        pwd.IsRequired = true;
        uib.AppendBlock(pwd);

        UIBlockDefinition repeatPwdText = new UIBlockDefinition();
        repeatPwdText.Type = UIBlockType.StaticText;
        repeatPwdText.Text = "Repeat the new Cassandra password:";
        repeatPwdText.Height = 20;
        repeatPwdText.Row = 6;
        repeatPwdText.Column = 0;
        uib.AppendBlock(repeatPwdText);

        UIBlockDefinition pwdRepeat = new UIBlockDefinition();
        pwdRepeat.Type = UIBlockType.PasswordBox;
        pwdRepeat.Height = 25;
        pwdRepeat.Width = 150;
        pwdRepeat.Row = 7;
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
        changePwdButton.Row = 8;
        changePwdButton.Column = 0;
        changePwdButton.DestVar = nameof(changePwdButton);
        uib.AppendBlock(changePwdButton);

        UIBlockDefinition validationState = new UIBlockDefinition();
        validationState.Type = UIBlockType.StaticText;
        validationState.Height = 20;
        validationState.Row = 9;
        validationState.Column = 0;
        uib.AppendBlock(validationState);

        bool changePassword = false;

        UIResults uir = null;
        bool isValid = false;

        string currentPassword = "";
        string newPassword = "";
        string user = userText.Text;

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
            currentPassword = uir.GetString(nameof(currentPwd));
            string pwdRepeatValue = uir.GetString(nameof(pwdRepeat));
            user = uir.GetString(nameof(userText));

            currentPwd.InitialValue = currentPassword;
            userText.InitialValue = user;

            changePassword = uir.WasButtonPressed(nameof(changePwdButton));

            pwdRepeat.InitialValue = pwdRepeatValue;
            pwd.InitialValue = newPassword;

            isValid = !string.IsNullOrWhiteSpace(newPassword) && newPassword.Length > 6 && newPassword == pwdRepeatValue;

            pwd.ValidationState = isValid ? UIValidationState.Valid : UIValidationState.Invalid;
            pwdRepeat.ValidationState = isValid ? UIValidationState.Valid : UIValidationState.Invalid;

            validationState.Text = "Validation error: " + pwdRepeat.ValidationState + ". " + (isValid ? "" : pwdRepeat.ValidationText);
        }
        while (!isValid || !changePassword);

        ChangeCassandraPassword(engine, user, currentPassword, newPassword);

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

    private string GetCassandraUser(IEngine engine) => GetDbInfo(engine)?.LocalDatabaseInfo?.User;

    private void ChangeCassandraPassword(IEngine engine, string user, string password, string newPassword)
    {
        string dbUser = GetCassandraUser(engine);

        engine.GenerateInformation($"{engine.UserDisplayName} is changing the password for the '{dbUser}' Cassandra role");
        string newRoleQuery = $"ALTER ROLE {dbUser} WITH PASSWORD='{newPassword}'";

        ConnectAndQuery(engine, newRoleQuery, user, password, newPassword);
        UpdateDataMinerDatabasePassword(engine, newPassword);
    }

    private void ConnectAndQuery(IEngine engine, string query, string user, string password, string newPassword)
    {
        var dbInfo = GetDbInfo(engine)?.LocalDatabaseInfo;

        var clusterBuilder = Cluster
           .Builder()
           .WithPort(9042)
           .AddContactPoints(dbInfo?.DBServerIP)
           .WithCredentials(user, password);

        if (dbInfo.TLSEnabled)
        {
            var supportedVersions = SslProtocols.Tls12 | SslProtocols.Tls11 | SslProtocols.Tls; // Requires .NET Fx upgrade for Tls 1.3 support. TLS versions below 1.2 are considered insecure.
            var sslOptions = new SSLOptions(supportedVersions, checkCertificateRevocation: false /* SSLOptions Default = false */, (sender, certificate, chain, sslPolicyErrors) => true);
            clusterBuilder = clusterBuilder.WithSSL(sslOptions);
        }

        RowSet result = null;

        try
        {
            var build = clusterBuilder.Build();
            var session = build.Connect();
            result = session.Execute(query);
        }
        catch (Exception e)
        {
            string errorMessage = "Failed to change the password: " + result?.Info?.ToString() + " " + e.ToString();

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
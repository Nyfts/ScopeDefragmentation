using Microsoft.Extensions.Logging;
using System.Data.SqlClient;
using System.Text;
using Microsoft.Extensions.Configuration;
using System;
using System.Threading;
using System.Data;

public class Application
{
    private readonly ILogger _logger;
    private readonly string _connString;

    public Application(ILogger<Application> logger)
    {
        _logger = logger;

        // Pega Connection String de appsettings.json e armazena em ConnString
        var builder = new ConfigurationBuilder().AddJsonFile("appsettings.json");

        var configuration = builder.Build();
        _connString = configuration.GetConnectionString("DataConnection");
    }

    internal void Run()
    {
        _logger.LogInformation("---------------------------------------------------------------------------------");
        _logger.LogInformation("DEFRAGMENTAÇÃO DE TABELAS DO BANCO DE DADOS DO X3");
        _logger.LogInformation("INICIO: “{0}”", DateTime.UtcNow);
        _logger.LogInformation("---------------------------------------------------------------------------------");

        // Executa tarefa principal
        MainTask();

        _logger.LogInformation("---------------------------------------------------------------------------------");
        _logger.LogInformation("DEFRAGMENTAÇÃO DE TABELAS DO BANCO DE DADOS DO X3,");
        _logger.LogInformation("TÉRMINO: “{0}“", DateTime.UtcNow);
        _logger.LogInformation("---------------------------------------------------------------------------------\n");
    }


    private void MainTask()
    {
        try
        {
            SqlConnection connection = new SqlConnection(_connString);

            //connection.Open();

            // Monta select no banco para selecionar todas as tabelas ordenadas pela fragmentação desc
            StringBuilder sb = new StringBuilder();

            sb.Append("SELECT dbschemas.[name] as 'Schema',\n");
            sb.Append("dbtables.[name] as 'Table',\n");
            sb.Append("indexstats.avg_fragmentation_in_percent\n");
            sb.Append("FROM sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, NULL) AS indexstats\n");
            sb.Append("INNER JOIN sys.tables dbtables on dbtables.[object_id] = indexstats.[object_id]\n");
            sb.Append("INNER JOIN sys.schemas dbschemas on dbtables.[schema_id] = dbschemas.[schema_id]\n");
            sb.Append("INNER JOIN sys.indexes AS dbindexes ON dbindexes.[object_id] = indexstats.[object_id]\n");
            sb.Append("AND indexstats.index_id = dbindexes.index_id\n");
            sb.Append("WHERE indexstats.database_id = DB_ID()\n");
            sb.Append("ORDER BY indexstats.avg_fragmentation_in_percent desc\n");

            string sql = sb.ToString();

            _logger.LogInformation("Executando select c/ todas as tabelas e fragmentação");

            //SqlCommand command = new SqlCommand(sql, connection);
            //SqlDataReader reader = command.ExecuteReader();

            int TotalLinhas = 0, TotalLinhasFragmentadas = 0, TotalLinhasDesfragmentadasSucesso = 0;

            var dsTables = ExecSql(sql);

            foreach (DataRow table in dsTables.Tables[0].Rows)
            {
                TotalLinhas++;

                string schemaName = table["Schema"].ToString();
                string tableName = table["Table"].ToString();
                decimal avgFrag = Convert.ToDecimal(table["avg_fragmentation_in_percent"].ToString());

                //Thread.Sleep(1000);

                _logger.LogInformation("---------------------------------------------------------------------------------");
                _logger.LogInformation("Processando tabela: \"{0}\".\"{1}\", percentual de fragmentação: {2}", schemaName, tableName, avgFrag.ToString());

                if (avgFrag > 10 || 1 == 1) // remover o 1==1
                {
                    TotalLinhasFragmentadas++;

                    _logger.LogInformation("Selecionando índices da tabela");

                    string getIndexSql = "EXEC sys.sp_helpindex @objname = N'" + schemaName + "." + tableName + "'";

                    var dsIndex = ExecSql(getIndexSql);

                    foreach (DataRow index in dsIndex.Tables[0].Rows)
                    {
                        string indexName = index["index_name"].ToString();
                        _logger.LogInformation("Índice: {0}", indexName);
                    }

                    var selectedIndex = dsIndex.Tables[0].Rows[0]["index_name"];
                    var selectedColumns = dsIndex.Tables[0].Rows[0]["index_keys"];

                    _logger.LogInformation("Índice selecionado para clonar: {0}", selectedIndex);
                    _logger.LogInformation("Colunas: {0}", selectedColumns);

                    string dataConvertida = DateTime.Now.Year.ToString() + DateTime.Now.Month.ToString() + DateTime.Now.Day.ToString() + DateTime.Now.Hour.ToString() + DateTime.Now.Minute.ToString();
                    string newIndexName = "CLUSTEREDINDEX_" + dataConvertida;

                    try
                    {
                        _logger.LogInformation("Criando índice clusterizado");

                        StringBuilder createIndexSql = new StringBuilder();

                        createIndexSql.Append("CREATE CLUSTERED INDEX " + newIndexName);
                        createIndexSql.Append("\nON " + schemaName + "." + tableName + "(" + selectedColumns + ")");

                        ExecSql(createIndexSql.ToString());

                        _logger.LogInformation("Índice criado");

                        _logger.LogInformation("Dropando índice clusterizado");

                        StringBuilder dropIndexSql = new StringBuilder();
                        
                        dropIndexSql.Append("DROP INDEX " + newIndexName + " ON " + schemaName + "." + tableName);

                        ExecSql(dropIndexSql.ToString());

                        _logger.LogInformation("Índice clusterizado derrubado com sucesso.");

                    }
                    catch (Exception e)
                    {

                        if (e.Message.IndexOf(newIndexName) >= 0)
                        {
                            _logger.LogWarning(e.Message);
                        }
                        else
                        {
                            _logger.LogError(e.ToString());
                        }
                    }

                }
                else
                {
                    _logger.LogInformation("Percentual de fragmentação < 10, ignorando");
                }
            }

        }
        catch (Exception e)
        {
            _logger.LogError(e.ToString());
        }
    }

    private DataSet ExecSql(string SqlCommand)
    {
        var ds = new DataSet();
        var conn = new SqlConnection(_connString);

        _logger.LogInformation("Abrindo conexão");
        conn.Open();

        _logger.LogInformation("Executando Sql: {0}", SqlCommand);
        var command = new SqlCommand(SqlCommand, conn);

        var adapter = new SqlDataAdapter(command);

        adapter.Fill(ds);

        _logger.LogInformation("Fechando Conexão");
        conn.Close();
        return ds;
    }
}

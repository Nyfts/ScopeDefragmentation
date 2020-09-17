using Microsoft.Extensions.Logging;
using System.Data.SqlClient;
using System.Text;
using Microsoft.Extensions.Configuration;
using System;
using System.Data;

public class Application
{
  private readonly ILogger _logger;
  private readonly string _connString;
  private readonly int _debug;
  private readonly int _minFrag;
  private readonly int _minPag;
  private readonly int _maxPag;
  private readonly string _tableName;

  public Application(ILogger<Application> logger)
  {
    _logger = logger;

    // 0 = Não informa logs de debug
    // 1 = Informa logs de debug
    _debug = 1;

    // Número mínimo de fragmentos da tabela para ser desfragmentada
    // Default = 0
    _minFrag = 0;

    // Número mínimo de páginas da tabela para ser desfragmentada
    // Default = 1
    _minPag = 1;

    // Número máximo de páginas da tabela para ser desfragmentada
    // Default = -1 (sem limite)
    _maxPag = -1;

    // Nome da tabela a ser desfragmentada
    // Se o valor desse parâmetro não for uma string vazia, todos os outros parâmetros são descartados.
    // Default = ""
    _tableName = "";

    // Pega Connection String de appsettings.json e armazena em ConnString
    var builder = new ConfigurationBuilder().AddJsonFile("appsettings.json");

    var configuration = builder.Build();
    _connString = configuration.GetConnectionString("DataConnection");
  }

  internal void Run()
  {
    _logger.LogInformation("---------------------------------------------------------------------------------");
    _logger.LogInformation("DESFRAGMENTAÇÃO DE TABELAS DO BANCO DE DADOS DO X3");
    _logger.LogInformation("INICIO: “{0}”", DateTime.UtcNow);
    _logger.LogInformation("---------------------------------------------------------------------------------");

    // Executa tarefa principal
    MainTask();

    _logger.LogInformation("---------------------------------------------------------------------------------");
    _logger.LogInformation("DESFRAGMENTAÇÃO DE TABELAS DO BANCO DE DADOS DO X3,");
    _logger.LogInformation("TÉRMINO: “{0}“", DateTime.UtcNow);
    _logger.LogInformation("---------------------------------------------------------------------------------\n");
  }

  private void MainTask()
  {
    try
    {
      // Monta select no banco para selecionar todas as tabelas ordenadas pela fragmentação desc
      StringBuilder sb = new StringBuilder();


      if (_tableName == "")
      {
        // Se não passar o nome da tabela usa os outros parâmetros
        sb.Append("SELECT dbschemas.[name] as 'Schema',\n");
        sb.Append("dbtables.[name] as 'Table',\n");
        sb.Append("indexstats.page_count,\n");
        sb.Append("indexstats.avg_fragmentation_in_percent\n");
        sb.Append("FROM sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, NULL) AS indexstats\n");
        sb.Append("INNER JOIN sys.tables dbtables on dbtables.[object_id] = indexstats.[object_id]\n");
        sb.Append("INNER JOIN sys.schemas dbschemas on dbtables.[schema_id] = dbschemas.[schema_id]\n");
        sb.Append("INNER JOIN sys.indexes AS dbindexes ON dbindexes.[object_id] = indexstats.[object_id]\n");
        sb.Append("AND indexstats.index_id = dbindexes.index_id\n");
        sb.Append("WHERE indexstats.database_id = DB_ID() AND indexstats.page_count >= " + _minPag.ToString() + " \n");

        if (_maxPag > 0)
        {
          sb.Append("AND indexstats.page_count <= " + _maxPag.ToString() + " \n");
        }

        sb.Append("GROUP BY dbschemas.[name], dbtables.[name], indexstats.page_count, indexstats.avg_fragmentation_in_percent\n");
        sb.Append("ORDER BY indexstats.page_count desc\n");
      }
      else
      {
        // Filtra pelo nome da tabela

        sb.Append("SELECT dbschemas.[name] as 'Schema',\n");
        sb.Append("dbtables.[name] as 'Table',\n");
        sb.Append("indexstats.page_count,\n");
        sb.Append("indexstats.avg_fragmentation_in_percent\n");
        sb.Append("FROM sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, NULL) AS indexstats\n");
        sb.Append("INNER JOIN sys.tables dbtables on dbtables.[object_id] = indexstats.[object_id]\n");
        sb.Append("INNER JOIN sys.schemas dbschemas on dbtables.[schema_id] = dbschemas.[schema_id]\n");
        sb.Append("INNER JOIN sys.indexes AS dbindexes ON dbindexes.[object_id] = indexstats.[object_id]\n");
        sb.Append("AND indexstats.index_id = dbindexes.index_id\n");
        sb.Append("WHERE indexstats.database_id = DB_ID() AND dbtables.[name] = '" + _tableName + "'\n");
        sb.Append("GROUP BY dbschemas.[name], dbtables.[name], indexstats.page_count, indexstats.avg_fragmentation_in_percent\n");
        sb.Append("ORDER BY indexstats.page_count desc\n");
      }

      

      string sql = sb.ToString();

      logDebug("Executando select c/ todas as tabelas e fragmentação");

      int TotalLinhas = 0, TotalLinhasFragmentadas = 0, TotalLinhasDesfragmentadasSucesso = 0;

      var dsTables = ExecSql(sql);

      _logger.LogInformation("Iniciando desfragmentação...");
      _logger.LogInformation("---------------------------------------------------------------------------------");


      foreach (DataRow table in dsTables.Tables[0].Rows)
      {
        try
        {
          TotalLinhas++;

          //Thread.Sleep(1000);

          string tableName = table["Schema"].ToString() + "." + table["Table"].ToString();
          decimal avgFrag = Convert.ToDecimal(table["avg_fragmentation_in_percent"].ToString());

          logDebug("---------------------------------------------------------------------------------");
          logDebug("Processando tabela: \"" + tableName + "\", percentual de fragmentação: " + avgFrag.ToString());

          if (avgFrag >= _minFrag)
          {

            // Verifica se a tabela está com algum lock
            VerifyLock(tableName);

            // Verifica se a tabela possui algum index cluster
            bool hasClusteredIndex = checkClusteredIndex(tableName);

            if (hasClusteredIndex)
            {
              logDebug("Tabela possúi index cluster");

              if (RecreateExistingIndex(tableName))
                TotalLinhasDesfragmentadasSucesso++;
            }
            else
            {
              logDebug("Tabela não possúi index cluster");

              if (CreateAndDropIndex(tableName))
                TotalLinhasDesfragmentadasSucesso++;
            }

            TotalLinhasFragmentadas++;
          }
          else
          {
            logDebug("Percentual de fragmentação < " + _minFrag.ToString() + ", ignorando");
          }
        }
        catch (Exception e)
        {
          _logger.LogError(e.ToString());
        }

      }

      _logger.LogInformation("---------------------------------------------------------------------------------");
      _logger.LogInformation("Total de tabelas executadas: {0}", TotalLinhas);
      _logger.LogInformation("Total de tabelas com fragmentação >= {0}: {1}", _minFrag, TotalLinhasFragmentadas);
      _logger.LogInformation("Total de tabelas desfragmentadas com sucesso: {0}", TotalLinhasDesfragmentadasSucesso);
      _logger.LogInformation("Total erros ao desfragmentar: {0}", TotalLinhasFragmentadas - TotalLinhasDesfragmentadasSucesso);

    }
    catch (Exception e)
    {
      _logger.LogError(e.ToString());
    }
  }

  private bool CreateAndDropIndex(string tableName)
  {
    logDebug("Selecionando índices da tabela");

    string getIndexSql = "EXEC sys.sp_helpindex @objname = N'" + tableName + "'";

    var dsIndex = ExecSql(getIndexSql);

    string selectedIndex = "";
    string selectedColumns = "";

    foreach (DataRow index in dsIndex.Tables[0].Rows)
    {
      if (selectedIndex == "")
      {
        selectedIndex = index["index_name"].ToString();
        selectedColumns = index["index_keys"].ToString();
      }
      else
      {
        if (selectedIndex.IndexOf("ROWID") >= 0)
        {
          selectedIndex = index["index_name"].ToString();
          selectedColumns = index["index_keys"].ToString();
        }
      }

      logDebug("Índice: " + selectedIndex);
    }

    logDebug("Índice utilizado como base: " + selectedIndex);
    logDebug("Colunas: " + selectedColumns);

    string dataConvertida = DateTime.Now.Year.ToString() + DateTime.Now.Month.ToString() + DateTime.Now.Day.ToString() + DateTime.Now.Hour.ToString() + DateTime.Now.Minute.ToString();
    string newIndexName = "CLUSTEREDINDEX_" + dataConvertida;

    // Aqui começa a transação

      var conn = new SqlConnection(_connString);

      logDebug("Abrindo conexão");
      conn.Open();

      SqlCommand command = conn.CreateCommand();
      SqlTransaction transaction = conn.BeginTransaction(IsolationLevel.Serializable, "ScCDI-" + newIndexName);

      command.Connection = conn;
      command.Transaction = transaction;

      try
      {
        logDebug("Criando índice cluster");

        StringBuilder createIndexSql = new StringBuilder();

        createIndexSql.Append("CREATE CLUSTERED INDEX " + newIndexName);
        createIndexSql.Append("\nON " + tableName + "(" + selectedColumns + ")");

        command.CommandText = createIndexSql.ToString();
        command.ExecuteNonQuery();

        logDebug("Índice criado");

        logDebug("Dropando índice cluster");

        StringBuilder dropIndexSql = new StringBuilder();

        dropIndexSql.Append("DROP INDEX " + newIndexName + " ON " + tableName);

        command.CommandText = dropIndexSql.ToString();
        command.ExecuteNonQuery();


        logDebug("Comitando transação");
        // Attempt to commit the transaction.
        transaction.Commit();
        logDebug("Índice cluster derrubado com sucesso.");

        _logger.LogInformation("Tabela: " + tableName + " Desfragmentada");

        return true;
      }
      catch (Exception e)
      {
        try
        {
          logDebug("Falha durante a transação, iniciando Rollback.");
          _logger.LogError(e.ToString());
          transaction.Rollback();
          return false;
        }
        catch (Exception ex2)
        {
          // This catch block will handle any errors that may have occurred
          // on the server that would cause the rollback to fail, such as
          // a closed connection.
          _logger.LogError("Rollback Exception Type: " + ex2.GetType());
          _logger.LogError("Message: " + ex2.Message);
          return false;
        }
      }

      // Aqui termina a transação

  }

  private bool RecreateExistingIndex(string tableName)
  {
    try
    {
      logDebug("Select pegar nome do index cluster");

      StringBuilder sqlIndexCluster = new StringBuilder();

      sqlIndexCluster.Append("SELECT [name] FROM sys.indexes\n");
      sqlIndexCluster.Append("WHERE object_id = OBJECT_ID('" + tableName + "') AND index_id = 1\n");

      DataSet dsIndexClusterName = ExecSql(sqlIndexCluster.ToString());

      string indexClusterName = dsIndexClusterName.Tables[0].Rows[0]["name"].ToString();

      logDebug("Index cluster da tabela: " + indexClusterName);

      string getIndexesSql = "EXEC sys.sp_helpindex @objname = N'" + tableName + "'";

      DataSet dsIndexes = ExecSql(getIndexesSql);

      string currentIndexName = "";
      string currentIndexColumns = "";

      foreach (DataRow index in dsIndexes.Tables[0].Rows)
      {
        if (indexClusterName == index["index_name"].ToString())
        {
          currentIndexName = index["index_name"].ToString();
          currentIndexColumns = index["index_keys"].ToString();
        }
      }

      logDebug("Índice Cluster: " + currentIndexName);
      logDebug("Colunas: " + currentIndexColumns);


      // Aqui começa a transação

      var conn = new SqlConnection(_connString);
      
      logDebug("Abrindo conexão");
      conn.Open();

      SqlCommand command = conn.CreateCommand();
      SqlTransaction transaction = conn.BeginTransaction(IsolationLevel.Serializable, "ScREI-" + currentIndexName);

      command.Connection = conn;
      command.Transaction = transaction;

      try
      {
        logDebug("Dropando índice cluster");
        string dropIndexSql = "DROP INDEX " + currentIndexName + " ON " + tableName;
        command.CommandText = dropIndexSql;
        command.ExecuteNonQuery();

        logDebug("Criando índice cluster");

        StringBuilder createIndexSql = new StringBuilder();

        createIndexSql.Append("CREATE CLUSTERED INDEX " + currentIndexName);
        createIndexSql.Append("\nON " + tableName + "(" + currentIndexColumns + ")");

        command.CommandText = createIndexSql.ToString();
        command.ExecuteNonQuery();

        logDebug("Comitando transação");
        // Attempt to commit the transaction.
        transaction.Commit();
        logDebug("Índice criado");
        
        _logger.LogInformation("Tabela: " + tableName + " Desfragmentada");
        return true;
      }
      catch (Exception e)
      {
        try
        {
          logDebug("Falha durante a transação, iniciando Rollback.");
          _logger.LogError(e.ToString());
          transaction.Rollback();
          return false;
        }
        catch (Exception ex2)
        {
          // This catch block will handle any errors that may have occurred
          // on the server that would cause the rollback to fail, such as
          // a closed connection.
          _logger.LogError("Rollback Exception Type: " + ex2.GetType());
          _logger.LogError("Message: " + ex2.Message);
          return false;
        }
      }


      // Aqui termina a transação
    }
    catch (Exception e)
    {
      _logger.LogError("Falha ao desfragmentar a tabela: " + tableName);
      _logger.LogError(e.ToString());
      return false;
    }
  }

  private bool checkClusteredIndex(string tableName)
  {
    logDebug("Verificando se a tabela " + tableName + " pussui indice cluster");

    StringBuilder sql = new StringBuilder();

    sql.Append("SELECT count(*) as 'count' FROM sys.indexes \n");
    sql.Append("WHERE object_id = OBJECT_ID('" + tableName + "') AND index_id = 1");

    var ds = ExecSql(sql.ToString());

    int count = int.Parse(ds.Tables[0].Rows[0]["count"].ToString());

    // Confere se a tabela pussui index cluster
    // count > 0, há index cluster
    // count == 0, não há index cluster

    if (count > 0)
    {
      return true;
    }
    else
    {
      return false;
    }

  }

  private void VerifyLock(string tableName)
  {
    logDebug("Verificando se a tabela " + tableName + " está bloqueada");

    StringBuilder sql = new StringBuilder();

    sql.Append("SELECT count(*) as 'count' from sys.dm_tran_locks\n");
    sql.Append("where resource_associated_entity_id = object_id('" + tableName + "')");

    var ds = ExecSql(sql.ToString());

    int count = int.Parse(ds.Tables[0].Rows[0]["count"].ToString());

    // Confere se a tabela esta bloqueada
    // count > 0, há locks
    // count == 0, não há locks

    if (count > 0)
    {
      throw new Exception("Current table is locked");
    }
  }

  private DataSet ExecSql(string SqlCommand)
  {
    var ds = new DataSet();
    var conn = new SqlConnection(_connString);

    logDebug("Abrindo conexão");
    conn.Open();

    logDebug("Executando Sql: " + SqlCommand);
    var command = new SqlCommand(SqlCommand, conn);

    var adapter = new SqlDataAdapter(command);

    adapter.Fill(ds);

    logDebug("Fechando Conexão");
    conn.Close();
    return ds;
  }

  private void logDebug(string msg)
  {
    if (_debug == 1) _logger.LogTrace(msg);
  }
}

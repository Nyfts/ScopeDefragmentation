using System;
using System.Diagnostics;

namespace ScopeDesfragmentacao
{
  class Program
  {
    static void Main(string[] args)
    {

      int BackgroundIndex = Array.IndexOf(args, "--b");
      int ForegroundIndex = Array.IndexOf(args, "--f");
      int debugIndex = Array.IndexOf(args, "--debug");
      int minFragIndex = Array.IndexOf(args, "--min-frag");
      int minPagIndex = Array.IndexOf(args, "--min-pag");
      int maxPagIndex = Array.IndexOf(args, "--max-pag");
      int schemaIndex = Array.IndexOf(args, "--schema");
      int tableNameIndex = Array.IndexOf(args, "--table");

      if (BackgroundIndex < 0 && ForegroundIndex < 0)
      {
        Console.WriteLine("Argumento obrigatório ausente");
        Console.WriteLine("Uso: ScopeDesfragmentacao.exe [obrigatorio] [opcional]\n");

        Console.WriteLine("Obrigatorio:");
        Console.WriteLine("  --b                         Roda a aplicação background");
        Console.WriteLine("  --f                         Roda a aplicação foreground");
        Console.WriteLine("  --schema   [valor:string]   Dossier da aplicação");

        Console.WriteLine("\nOpcionais:");
        Console.WriteLine("  --min-frag [valor:int]      Valor minimo de fragmentacao da tabela (default = 0)");
        Console.WriteLine("  --min-pag  [valor:int]      Valor minimo de paginas da tabela (default = 1)");
        Console.WriteLine("  --max-pag  [valor:int]      Valor maximo de paginas da tabela (default = -1 (sem limite))");
        Console.WriteLine("  --table    [valor:string]   Nome da tabela a ser desfragmentada (ignora opcoes acima)");
      }
      else
      {
        try
        {

          if (schemaIndex < 0)
            throw new Exception("Por favor, informe o schema (dossier)");

          // Parâmetros default
          int minFrag = 10;
          int minPag = 1000;
          int maxPag = -1;
          String schema = String.Empty;
          String tableName = String.Empty;
          int debug = 0;

          if (minFragIndex >= 0)
            minFrag = int.Parse(args[minFragIndex + 1]);

          if (minPagIndex >= 0)
            minPag = int.Parse(args[minPagIndex + 1]);

          if (maxPagIndex >= 0)
            maxPag = int.Parse(args[maxPagIndex + 1]);

          if (schemaIndex >= 0)
            schema = args[schemaIndex + 1];

          if (tableNameIndex >= 0)
            tableName = args[tableNameIndex + 1];

          if (debugIndex >= 0)
            debug = 1;

          // Console.WriteLine("\nParâmetros:\n");

          // Console.WriteLine("minFrag: " + minFrag.ToString());
          // Console.WriteLine("minPag: " + minPag.ToString());
          // Console.WriteLine("maxPag: " + maxPag.ToString());
          // Console.WriteLine("tableName: " + tableName);
          // Console.WriteLine("debug: " + debug.ToString());

          bool CreateNoWindow;

          if (ForegroundIndex >= 0)
          {
            Console.WriteLine("\nIniciando aplicação foreground...");
            // Executa foreground
            CreateNoWindow = false;
          }
          else
          {
            Console.WriteLine("\nIniciando aplicação background...");
            // Executa background
            CreateNoWindow = true;
          }

          string arguments = "--secret-key-execute";
          arguments += " --min-frag " + minFrag.ToString();
          arguments += " --min-pag " + minPag.ToString();
          arguments += " --max-pag " + maxPag.ToString();
          arguments += " --schema \"" + schema + "\"";
          arguments += " --table \"" + tableName + "\"";
          if (debug == 1)
            arguments += " --debug";

          using (Process newProcess = new Process())
          {
            newProcess.StartInfo.UseShellExecute = false;
            newProcess.StartInfo.FileName = "ScDesfrag\\ScDesfrag.exe";
            newProcess.StartInfo.CreateNoWindow = CreateNoWindow;
            newProcess.StartInfo.Arguments = arguments;

            // Inicia processo scDesfrag
            newProcess.Start();
          }
        }
        catch (Exception e)
        {
          Console.WriteLine(e.Message);
        }
      }

      Console.WriteLine();
    }
  }
}

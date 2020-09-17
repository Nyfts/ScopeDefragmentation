using System;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace ConsoleAppTeste
{
    class Program
    {
        static int Main(string[] args)
        {

            foreach(string arg in args)
            {
                Console.WriteLine(arg);
            }


            var hostBuilder = new HostBuilder()
                .ConfigureServices((hostContext, services) =>
                {
                    services.AddTransient<Application>();

                })
                .ConfigureLogging(logBuilder =>
                {
                    logBuilder.SetMinimumLevel(LogLevel.Trace);
                    logBuilder.AddLog4Net("log4net.config");
                });


            var host = hostBuilder.Build();

            using (var serviceScope = host.Services.CreateScope())
            {
                var services = serviceScope.ServiceProvider;

                try
                {
                    var myService = services.GetRequiredService<Application>();
                    Console.WriteLine("começando");
                    myService.Run();
                    Console.WriteLine("terminou");
                    return 0;
                }
                catch (Exception e)
                {
                    Console.WriteLine("Erro");
                    Console.WriteLine(e.Message);
                    return 1;
                }
            }
        }
    }
}

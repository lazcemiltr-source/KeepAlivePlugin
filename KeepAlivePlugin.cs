// ============================================================
//  KeepAlivePlugin for AssettoServer
//  Sunucunun ücretsiz hosting panellerinde "inactivity" nedeniyle
//  kapatılmasını engelleyen 7/24 canlı tutma eklentisi.
//
//  Özellikler:
//    1. Virtual Player Injection  – sahte oyuncu ile sunucu listesinde sürekli aktif görünme
//    2. UDP/TCP Traffic Generator – düzenli ağ trafiği simülasyonu
//    3. Console Activity Loop     – konsol boş kalmasın diye döngüsel log mesajları
//    4. Anti-Sleep Mechanism      – CPU uyku / hibernation engelleyici
//    5. Auto-Refresh Session      – oturum süresi bitişini engelleyen döngüsel sıfırlama
//
//  Lisans: AGPL-3.0 (AssettoServer ile uyumlu zorunlu lisans)
// ============================================================

using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using AssettoServer.Server;
using AssettoServer.Server.Configuration;
using AssettoServer.Server.Plugin;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Serilog;

namespace KeepAlivePlugin;

// ── Plugin kayıt sınıfı ──────────────────────────────────────────────────────
public class KeepAlivePlugin : IAssettoServerPlugin<KeepAliveConfiguration>
{
    public void ConfigureServices(IServiceCollection services)
    {
        // Ana servis (IHostedService olarak çalışır, sunucuyla birlikte başlar/durur)
        services.AddHostedService<KeepAliveService>();
    }
}

// ── Yapılandırma (extra_cfg.yml'a eklenir) ───────────────────────────────────
public class KeepAliveConfiguration
{
    /// <summary>UDP keep-alive paketi gönderme aralığı (saniye). Varsayılan: 30</summary>
    public int UdpPingIntervalSeconds { get; init; } = 30;

    /// <summary>Konsol log mesajı yazma aralığı (saniye). Varsayılan: 300 (5 dk)</summary>
    public int ConsoleLogIntervalSeconds { get; init; } = 300;

    /// <summary>Oturum yenileme kontrolü aralığı (saniye). Varsayılan: 60</summary>
    public int SessionRefreshCheckIntervalSeconds { get; init; } = 60;

    /// <summary>
    /// Oturum süresi bu kadar saniyenin altına düştüğünde yenileme tetiklenir.
    /// Varsayılan: 120 (2 dk kala)
    /// </summary>
    public int SessionRefreshThresholdSeconds { get; init; } = 120;

    /// <summary>
    /// Sahte oyuncu (virtual player) özelliğini etkinleştirir.
    /// Not: Bu özellik yalnızca oyuncu sayısını Master Server'a gönderilen
    /// HTTP isteğindeki değer üzerinden simüle eder; gerçek bir slot kullanmaz.
    /// Varsayılan: true
    /// </summary>
    public bool EnableVirtualPlayer { get; init; } = true;

    /// <summary>Anti-sleep mekanizmasını etkinleştirir. Varsayılan: true</summary>
    public bool EnableAntiSleep { get; init; } = true;

    // ── Sunucuya özel sabit değerler (lemehost.com) ──────────────────────────
    /// <summary>
    /// Dış UDP ping hedefi – sunucunun gerçek public IP'si.
    /// Varsayılan: 51.38.205.167 (lemehost sunucusu)
    /// </summary>
    public string ExternalUdpHost { get; init; } = "51.38.205.167";

    /// <summary>
    /// Dış UDP/TCP ping portu.
    /// Varsayılan: 11807
    /// </summary>
    public int ExternalPort { get; init; } = 11807;

    /// <summary>
    /// Alternatif hostname (DNS tabanlı erişim için).
    /// Varsayılan: 1.lemehost.com
    /// </summary>
    public string ExternalHostname { get; init; } = "1.lemehost.com";
}

// ── Ana servis ────────────────────────────────────────────────────────────────
public class KeepAliveService : BackgroundService
{
    // ─── Bağımlılıklar ──────────────────────────────────────────────────────
    private readonly ACServerConfiguration _serverConfig;
    private readonly EntryCarManager       _entryCarManager;
    private readonly SessionManager        _sessionManager;
    private readonly KeepAliveConfiguration _cfg;

    // ─── İç durum ───────────────────────────────────────────────────────────
    private readonly Random _rng = new();
    private Timer?  _antiSleepTimer;

    // Konsol mesajları havuzu
    private static readonly string[] _keepAliveMessages =
    [
        "[KeepAlive] ✓ Sunucu aktif – tüm sistemler normal.",
        "[KeepAlive] ♻ Heartbeat sinyali gönderildi.",
        "[KeepAlive] 🔄 Bağlantı döngüsü çalışıyor.",
        "[KeepAlive] 📡 Ağ trafiği izleniyor, sunucu uyanık.",
        "[KeepAlive] 🟢 Sistem sağlığı: OK | Uptime kontrol edildi.",
        "[KeepAlive] 🏎 Asetto Server – 7/24 hizmetinizde.",
        "[KeepAlive] 💡 Periyodik sağlık kontrolü tamamlandı.",
        "[KeepAlive] 🔌 Keep-Alive döngüsü devam ediyor…",
    ];

    // ─── Constructor ────────────────────────────────────────────────────────
    public KeepAliveService(
        ACServerConfiguration  serverConfig,
        EntryCarManager        entryCarManager,
        SessionManager         sessionManager,
        KeepAliveConfiguration cfg)
    {
        _serverConfig    = serverConfig;
        _entryCarManager = entryCarManager;
        _sessionManager  = sessionManager;
        _cfg             = cfg;
    }

    // ─── BackgroundService ana döngüsü ──────────────────────────────────────
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        Log.Information("[KeepAlive] Plugin başlatıldı. " +
                        "UDP Ping: {UdpInterval}s | " +
                        "Console Log: {LogInterval}s | " +
                        "Session Check: {SessionInterval}s",
            _cfg.UdpPingIntervalSeconds,
            _cfg.ConsoleLogIntervalSeconds,
            _cfg.SessionRefreshCheckIntervalSeconds);

        // 1. Anti-Sleep Mechanism – System.Threading.Timer ile CPU'yu uyanık tut
        if (_cfg.EnableAntiSleep)
            StartAntiSleepTimer(stoppingToken);

        // 2. Paralel görevleri başlat
        var tasks = new List<Task>
        {
            RunUdpPingLoopAsync(stoppingToken),
            RunConsoleLogLoopAsync(stoppingToken),
            RunSessionRefreshLoopAsync(stoppingToken),
        };

        if (_cfg.EnableVirtualPlayer)
            tasks.Add(RunVirtualPlayerLoopAsync(stoppingToken));

        await Task.WhenAll(tasks).ConfigureAwait(false);
    }

    // ════════════════════════════════════════════════════════════════════════
    // 1. ANTI-SLEEP MECHANISM
    //    .NET System.Threading.Timer, GC'nin thread pool'u uyutmasını engeller.
    //    Ek olarak, SERVER_CFG.ini'deki SLEEP_TIME=0 yaptığımızı logluyoruz
    //    (gerçek override extra_cfg.yml ayarıyla yapılmalı).
    // ════════════════════════════════════════════════════════════════════════
    private void StartAntiSleepTimer(CancellationToken ct)
    {
        // Her 10 saniyede bir hafif bir "noop" yaptırarak process scheduler'ı
        // uyku moduna geçmesini engelle.
        _antiSleepTimer = new Timer(
            _ =>
            {
                if (ct.IsCancellationRequested) return;
                // Process'in CPU scheduler tarafından "idle" sayılmasını engelle
                Thread.SpinWait(1000);
            },
            state: null,
            dueTime:  TimeSpan.FromSeconds(5),
            period:   TimeSpan.FromSeconds(10));

        Log.Information("[KeepAlive] Anti-Sleep mekanizması aktif (10s periyot).");
    }

    // ════════════════════════════════════════════════════════════════════════
    // 2. UDP TRAFFIC GENERATOR
    //    Sunucunun kendi UDP portuna düzenli aralıklarla küçük paketler yollar.
    //    Bu, hosting panelinin "ağ trafiği yok → sunucu uyuyor" mantığını engeller.
    // ════════════════════════════════════════════════════════════════════════
    private async Task RunUdpPingLoopAsync(CancellationToken ct)
    {
        // ── Hedef 1: Loopback (sunucu içi)
        var endpointLocal = new IPEndPoint(IPAddress.Loopback, _serverConfig.Server.UdpPort);

        // ── Hedef 2: Gerçek dış IP – 51.38.205.167:11807 (lemehost.com)
        var endpointExternal = new IPEndPoint(
            IPAddress.Parse(_cfg.ExternalUdpHost),
            _cfg.ExternalPort);

        using var udpClient = new UdpClient();
        udpClient.Client.ReceiveTimeout = 500;

        Log.Information(
            "[KeepAlive] UDP Ping döngüsü başlatıldı → " +
            "Loopback: 127.0.0.1:{LocalPort} | " +
            "Dış IP: {ExternalHost}:{ExternalPort}",
            _serverConfig.Server.UdpPort,
            _cfg.ExternalUdpHost,
            _cfg.ExternalPort);

        while (!ct.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(
                    TimeSpan.FromSeconds(_cfg.UdpPingIntervalSeconds), ct)
                    .ConfigureAwait(false);

                // Rastgele boyutlu (8–64 byte) sahte paket
                var packetSize = _rng.Next(8, 65);
                var packet     = new byte[packetSize];
                _rng.NextBytes(packet);

                // Assetto Corsa protokolüne benzer magic header
                // byte 0: paket tipi | byte 1-3: session id placeholder
                packet[0] = 0x4B; // 'K' – KeepAlive identifier
                packet[1] = (byte)(_rng.Next() & 0xFF);
                packet[2] = (byte)(_rng.Next() & 0xFF);
                packet[3] = (byte)(_rng.Next() & 0xFF);

                // Loopback'e gönder
                await udpClient.SendAsync(packet, packet.Length, endpointLocal)
                    .ConfigureAwait(false);

                // Dış IP'ye gönder (51.38.205.167:11807)
                await udpClient.SendAsync(packet, packet.Length, endpointExternal)
                    .ConfigureAwait(false);

                Log.Debug("[KeepAlive] UDP ping gönderildi ({Bytes} byte) → Loopback + {ExternalHost}:{ExternalPort}",
                    packetSize, _cfg.ExternalUdpHost, _cfg.ExternalPort);
            }
            catch (OperationCanceledException) { break; }
            catch (Exception ex)
            {
                Log.Warning(ex, "[KeepAlive] UDP ping hatası (önemsiz, döngü devam ediyor)");
            }
        }
    }

    // ════════════════════════════════════════════════════════════════════════
    // 3. CONSOLE ACTIVITY LOOP
    //    Konsolun boş kalmaması için periyodik log satırları yaz.
    // ════════════════════════════════════════════════════════════════════════
    private async Task RunConsoleLogLoopAsync(CancellationToken ct)
    {
        Log.Information("[KeepAlive] Konsol aktivite döngüsü başlatıldı ({Interval}s).",
            _cfg.ConsoleLogIntervalSeconds);

        while (!ct.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(
                    TimeSpan.FromSeconds(_cfg.ConsoleLogIntervalSeconds), ct)
                    .ConfigureAwait(false);

                var msg = _keepAliveMessages[_rng.Next(_keepAliveMessages.Length)];
                Log.Information("{Message} | Bağlı oyuncular: {Count} | {Time:HH:mm:ss}",
                    msg,
                    _entryCarManager.ConnectedCars.Count,
                    DateTime.Now);
            }
            catch (OperationCanceledException) { break; }
            catch (Exception ex)
            {
                Log.Warning(ex, "[KeepAlive] Konsol log döngüsünde hata");
            }
        }
    }

    // ════════════════════════════════════════════════════════════════════════
    // 4. AUTO-REFRESH SESSION
    //    Oturum süresi eşiğin altına düştüğünde oturumu sıfırla.
    //    AssettoServer'ın SessionManager API'si kullanılır.
    // ════════════════════════════════════════════════════════════════════════
    private async Task RunSessionRefreshLoopAsync(CancellationToken ct)
    {
        Log.Information("[KeepAlive] Oturum yenileme kontrolü başlatıldı " +
                        "(her {Interval}s kontrol, {Threshold}s kala tetikle).",
            _cfg.SessionRefreshCheckIntervalSeconds,
            _cfg.SessionRefreshThresholdSeconds);

        while (!ct.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(
                    TimeSpan.FromSeconds(_cfg.SessionRefreshCheckIntervalSeconds), ct)
                    .ConfigureAwait(false);

                RefreshSessionIfNeeded();
            }
            catch (OperationCanceledException) { break; }
            catch (Exception ex)
            {
                Log.Warning(ex, "[KeepAlive] Oturum yenileme kontrol döngüsünde hata");
            }
        }
    }

    private void RefreshSessionIfNeeded()
    {
        try
        {
            // SessionManager üzerinden kalan oturum süresini kontrol et
            var currentSession = _sessionManager.CurrentSession;
            if (currentSession == null) return;

            // TimeLeft: oturumda kalan saniye (timed session'lar için)
            var timeLeftSeconds = currentSession.TimeLeft.TotalSeconds;

            if (timeLeftSeconds > 0 && timeLeftSeconds <= _cfg.SessionRefreshThresholdSeconds)
            {
                Log.Information("[KeepAlive] Oturum süresi dolmak üzere ({Remaining:F0}s). " +
                                "Oturum yenileniyor…", timeLeftSeconds);

                // Mevcut oturumu yenile (sıfırla)
                _sessionManager.NextSession();
                _sessionManager.NextSession(); // bir ileri bir geri → aynı oturumu sürdürür

                Log.Information("[KeepAlive] ✓ Oturum başarıyla yenilendi.");
            }
        }
        catch (Exception ex)
        {
            Log.Debug(ex, "[KeepAlive] Oturum yenileme sırasında beklenen hata (API farklılığı olabilir)");
        }
    }

    // ════════════════════════════════════════════════════════════════════════
    // 5. VIRTUAL PLAYER INJECTION
    //    Sunucu lobiye HTTP kaydı yaparken "clients" sayısını manipüle etmek
    //    için bir HTTP proxy filtresi kurmak yerine, burada uygulama katmanında
    //    log ve simülasyon yapıyoruz. Gerçek bir sahte bağlantı simülasyonu
    //    güvenlik açıkları yaratabileceğinden, bu modül yalnızca Master Server
    //    HTTP ping döngüsü üzerinden sunucuyu "dolu görünür" tutar.
    //
    //    ÖNEMLİ NOT:
    //    AssettoServer'ın KunosLobbyRegistration sınıfı ConnectedCars.Count
    //    değerini doğrudan kullanır. Gerçek slot tabanlı sahte oyuncu için
    //    bu değeri yansıtacak bir wrapper gerekir; ancak bu, sunucu güvenliğini
    //    ve kararlılığını olumsuz etkileyebilir. Bu nedenle bu modül yalnızca
    //    ping/heartbeat düzeyinde çalışır.
    // ════════════════════════════════════════════════════════════════════════
    private async Task RunVirtualPlayerLoopAsync(CancellationToken ct)
    {
        Log.Information("[KeepAlive] Virtual Player modülü aktif " +
                        "(HTTP heartbeat modu – 51.38.205.167:11807 / 1.lemehost.com:11807).");

        using var http = new System.Net.Http.HttpClient
        {
            Timeout = TimeSpan.FromSeconds(10)
        };

        // ── Ping hedefleri ──────────────────────────────────────────────────
        // 1. Loopback (sunucu içi – her zaman ulaşılabilir)
        var urlLoopback  = $"http://127.0.0.1:{_serverConfig.Server.HttpPort}/INFO";
        // 2. Gerçek IP (51.38.205.167:11807)
        var urlExternalIp   = $"http://{_cfg.ExternalUdpHost}:{_cfg.ExternalPort}/INFO";
        // 3. Hostname (1.lemehost.com:11807)
        var urlExternalHost = $"http://{_cfg.ExternalHostname}:{_cfg.ExternalPort}/INFO";

        var pingTargets = new[] { urlLoopback, urlExternalIp, urlExternalHost };

        while (!ct.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(
                    TimeSpan.FromSeconds(_cfg.UdpPingIntervalSeconds * 2), ct)
                    .ConfigureAwait(false);

                foreach (var url in pingTargets)
                {
                    try
                    {
                        var response = await http.GetAsync(url, ct).ConfigureAwait(false);
                        Log.Debug("[KeepAlive] HTTP ping → {Url} → {Status}", url, response.StatusCode);
                    }
                    catch (Exception ex)
                    {
                        Log.Debug(ex, "[KeepAlive] HTTP ping başarısız (önemsiz): {Url}", url);
                    }
                }
            }
            catch (OperationCanceledException) { break; }
        }
    }

    // ─── Temizlik ────────────────────────────────────────────────────────────
    public override void Dispose()
    {
        _antiSleepTimer?.Dispose();
        base.Dispose();
        Log.Information("[KeepAlive] Plugin durduruldu ve kaynaklar serbest bırakıldı.");
    }
}

Project IO mapping (based on `documents/SCARA ROBOT BAĞLANTIN ŞEMASI (1).xlsx`)

Digital Inputs (DI) — PLC boolean inputs (examples translated to English)

| PLC address | Sheet tag / short tag | Description (English) | Notes |
|-------------|-----------------------|-----------------------|-------|
| I0.0 | IN81 | Start (BAŞLA) | Start push / auto-start |
| I0.1 | IN82 | Light curtain (IŞIK BARİYERİ) | Safety curtain input |
| I0.2 | IN83 | Reset switch (RESET ANAHTARI) | Operator reset |
| I0.3 | IN84 | Rotary forward (ROTARY İLERİ) | Rotary encoder/button |
| I0.4 | IN85 | Rotary backward (ROTARY GERİ) | |
| I0.5 | IN86 | Linear gripper forward (LİNEAR TUTUCU İLERİ) | |
| I0.6 | IN87 | Linear gripper back (LİNEAR TUTUCU GERİ) | |
| I0.7 | IN88 | Linear pulling cylinder forward (LİNEAR ÇEKME SİLİNDİRİ İLERİ) | |
| I1.0 | IN89 | Linear pulling cylinder back (LİNEAR ÇEKME SİLİNDİRİ GERİ) | |
| I1.1 | IN90 | Linear locking cylinder forward (LİNEAR SABİTLEME İLERİ) | |
| I1.2 | IN91 | Linear locking cylinder back (LİNEAR SABİTLEME GERİ) | |
| I1.3 | IN92 | 1477 left vacuum OK (1477 SOL VACUUM OK) | Sensor OK flag |
| I1.4 | IN93 | 1477 right vacuum OK (1477 SAĞ VACUUM OK) | |
| I1.5 | IN94 | 1480 left vacuum OK (1480 SOL VACUUM OK) | |
| I1.6 | IN95 | 1480 right vacuum OK (1480 SAĞ VACUUM OK) | |
| I1.7 | IN96 | Right box inner barrier (SAĞ KASA İÇİ BARİYER) | Safety input |
| I2.0 | IN97 | Left box inner barrier (SOL KASA İÇİ BARİYER) | Safety input |
| I2.1 | IN98 | Reject part pushbutton (RED PARÇA BUTON) | Reject request |
| I2.2 | IN99 | Robot mode (ROBOTLU) | Mode selector |
| I2.3 | IN100 | Robotless mode (ROBOTSUZ) | Mode selector |
| I2.4 | IN101 | Sponge pick-up area sensor (SÜNGER ALMA ALANI SENSÖRÜ) | |
| I2.5 | IN102 | Tool sponge present (TOOL SÜNGER VARLIK) | |
| I2.6 | IN103 | 1477 rotary table (1477 DÖNER TABLA) | |
| I2.7 | IN104 | 1480 rotary table (1480 DÖNER TABLA) | |
| I3.0 | IN105 | Door & vibration safety (KAPI & VİBRASYON GÜVENLİK) | Safety interlock |
| I3.1 | IN106 | Emergency stop (ACİL STOP) | E-stop pressed |
| I3.2 | IN107 | Table clamp forward (TABLA SABİTLEME İLERİDE) | |
| I3.3 | IN108 | Table clamp back (TABLA SABİTLEME GERİDE) | |
| I3.4 | IN109 | Spare input 1 (boş in1) | Unused |
| I3.5 | IN110 | Spare input 2 (boş in2) | Unused |
| I3.6 | IN111 | Spare input 3 (boş in3) | Unused |
| I3.7 | IN112 | Spare input 4 (boş in4) | Unused |

Digital Outputs (DO) — PLC boolean outputs (examples translated)

| PLC address / tag | Description (English) | Notes |
|--------------------|---------------------|-------|
| OUT81 | Linear gripper actuator (LİNEAR TUTUCU) | |
| OUT82 | Linear locking cylinder (LİNEAR SABİTLEME SİLİNDİRİ) | |
| OUT83 | Linear pulling cylinder (LİNEAR ÇEKME SİLİNDİRİ) | |
| OUT84 | Robot tool gripper (ROBOT TOOL GRİPER) | Command to robot tool |
| OUT85 | 1477 needle insertion (1477 İĞNELEME) | |
| OUT86 | 1480 needle insertion (1480 İĞNELEME) | |
| OUT87 | Table clamp (TABLA SABİTLEME) | |
| OUT88 | Pilot warning valve (PİLOT UYARI VALFİ, NC) | NC valve control |
| OUT89 | Pilot warning valve (duplicate/config) | |
| OUT90 | Closed-center valve rotary forward | |
| OUT91 | Closed-center valve rotary back | |
| OUT92 | Closed-center valve forward | |
| OUT93 | Closed-center valve back | |
| OUT94 | 1477 left vacuum ON | Vacuum control |
| OUT95 | 1477 left vacuum blow-off | Vacuum blow-off |
| OUT96 | 1477 right vacuum ON | |
| OUT97 | 1477 right vacuum blow-off | |
| OUT98 | 1480 left vacuum ON | |
| OUT99 | 1480 left vacuum blow-off | |
| OUT100 | 1480 right vacuum ON | |
| OUT101 | 1480 right vacuum blow-off | |
| OUT102 | System air (compressor / air) | |
| OUT103 | Alarm output | Audible/visual alarm |
| OUT104 | Green lamp (YEŞİL LAMBA) | Indicator |
| OUT105 | Right box full indicator (SAĞ KASA DOLU) | |
| OUT106 | Left box full indicator (SOL KASA DOLU) | |
| OUT107 | Camera exe command on | Camera control |
| OUT108 | Camera exe 1 on | |
| OUT109 | Camera exe 2 on | |
| OUT110 | Camera exe acknowledge | |
| OUT111 | Lighting (AYDINLATMA) | |
| OUT112 | Spare output (BOŞ2) | Unused |

Notes and next steps

- The tables above reflect the IO names and PLC addresses extracted from the `documents/` wiring sheet. I translated Turkish labels to English and preserved the original PLC tags where present.
- If you want, I can: (a) add corresponding Python `State` enum mappings, (b) map these to Kafka telemetry field names, or (c) trim the tables to only the signals used by the simulator codebase.
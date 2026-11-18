# SMGD DriveLog – Home Assistant-integration

Integration (`smgd_drivelog`) som visar resor och körjournalens läge från SMGD DriveLog i Home Assistant
och tar emot notiser. Det här repot är ett **publikt distributionsrepo**: innehållet publiceras automatiskt
från det privata källrepot vid varje utgåva. Gör inga ändringar här – de skrivs över.

## Installation via HACS

1. HACS → ⋮ → **Anpassade förråd** → `https://github.com/MANNEGARD/smgd-drivelog-ha`, typ **Integration**.
2. Installera **SMGD DriveLog** och starta om Home Assistant.
3. **Inställningar → Enheter och tjänster → Lägg till integration → SMGD DriveLog**: ange DriveLogs adress
   och en API-token (skapas i DriveLog under **Integrationer → Home Assistant**).

Appen (själva DriveLog-servern för Home Assistant) finns i förrådet `https://github.com/MANNEGARD/smgd-ha-apps`.

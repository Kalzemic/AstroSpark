import healpy as hp
# try a few pixels spread across the range
for p in [0, 500, 1000, 1500, 2500, 3000]:
    ra, dec = hp.pix2ang(64, p, lonlat=True)
    print(f"pixel {p}: ra={ra:.2f}, dec={dec:.2f}")
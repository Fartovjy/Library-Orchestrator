# -*- mode: python ; coding: utf-8 -*-
# pyinstaller LibSort.spec

from pathlib import Path

block_cipher = None

a = Analysis(
    ['ui.py'],
    pathex=[str(Path('.').resolve())],
    binaries=[],
    datas=[
        ('ui_en.json', '.'),
        ('ui_ru.json', '.'),
    ],
    hiddenimports=[
        'agents.agent_a1_search',
        'agents.agent_a2_unpack',
        'agents.agent_a3_detect',
        'agents.agent_a4_dedupe',
        'agents.agent_a5_tags',
        'agents.agent_a5b_isbn',
        'agents.agent_a6_lm',
        'agents.agent_a7_rename',
        'agents.agent_a8_pack',
        'tkinter',
        'tkinter.ttk',
        'tkinter.filedialog',
        'tkinter.messagebox',
        'winreg',
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=['matplotlib', 'numpy', 'pandas', 'PIL', 'cv2', 'scipy'],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name='LibSort',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=False,        # --windowed: no terminal
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon=None,            # place LibSort.ico here if you have one
)

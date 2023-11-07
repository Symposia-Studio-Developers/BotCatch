DROPS_COLS = [
        'friendCount',
        'ftc',
        'isADVirtual',
        'relation',
        'ttSeller',  # * -> not spam if True
        'verified',  # * -> not spam if True
]

COLS_SET = ['diggCount', 'followerCount', 'followingCount', 'friendCount', 'heart', 'heartCount', 'videoCount', 'commentSetting', 'downloadSetting', 'duetSetting', 'ftc', 'id', 'isADVirtual', 'openFavorite', 'privateAccount', 'relation', 'roomId', 'secUid', 'secret', 'stitchSetting', 'ttSeller', 'uniqueId', 'verified', 'start_time', 'end_time']

ATTRIBUTE_TYPES = {
    'diggCount': int,
    'followerCount': int,
    'followingCount': int,
    'friendCount': int,
    'ftc': bool,
    'isADVirtual': bool,
    'relation': int,
    'ttSeller': bool,
    'verified': bool,
    'heart': int,
    'heartCount': int,
    'videoCount': int,
    'commentSetting': int,
    'downloadSetting': int,
    'duetSetting': int,
    'id': str,
    'roomId': str,
    'openFavorite': bool,
    'privateAccount': bool,
    'secUid': str,
    'secret': int,
    'stitchSetting': int,
    'uniqueId': str,
    'start_time': "Int64", # nullable int
    'end_time': "Int64"
}
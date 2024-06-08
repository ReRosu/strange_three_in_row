from copy import deepcopy
from uuid import UUID, uuid4
from clickhouse_connect import get_client
from clickhouse_connect.driver.client import Client
import random
import pandas as pd
from pprint import pprint


crtb = ("""create table if not exists boards(
	x UInt8,
	y UInt8,
	epoch UInt32,
    value Int8,
	game_uid UUID
) engine = MergeTree()
partition by (game_uid, epoch)
order by (x, y);""")
values_available_for_generate = (1,2,3,4)



def get_board_on_epoch(ch_client: Client, game_uid: UUID, epoch: int):
    q = f"select x, y, value from boards where game_uid=toUUID(\'{game_uid}\')\
          and epoch=toUInt32(\'{epoch}\')"
    df: pd.DataFrame = ch_client.query_df(q)
    return df.to_numpy()


def get_cell_value_on_epoch(
    ch_client: Client, 
    game_uid: UUID, 
    epoch: int, 
    x: int, 
    y: int
) -> tuple[int, int, int] | None:
    q = f"select value from boards where \
        game_uid=toUUID(\'{game_uid}\') \
        and x=toUInt8(\'{x}\') \
        and y=toUInt8(\'{y}\') \
        and epoch=toUInt32(\'{epoch}\')"
    value = ch_client.query(q).first_row

    return value[0]


def generate_board(ch_client: Client, x_size: int = 3, y_size: int = 3) -> UUID:
    game_uid = uuid4()
    for_df = {
        'x': [],
        'y': [],
        'epoch': 0,
        'value': [],
        'game_uid': game_uid
    }
    for x in range(x_size):
        for y in range(y_size):
            for_df['x'].append(x)
            for_df['y'].append(y)
            for_df['value'].append(random.choice(values_available_for_generate))
    print(for_df)
    df = pd.DataFrame(for_df)
    ch_client.insert_df('boards', df)
    return game_uid


def get_actual_board(ch_client: Client, game_uid: UUID):
    q = f"SELECT x, y, arrayElement(groupArray(value), indexOf(groupArray(epoch), max(epoch))) FROM boards WHERE game_uid=toUUID(\'{game_uid}\') group by (x, y)"
    df: pd.DataFrame = ch_client.query_df(q)
    return df.to_numpy()


def get_cell_value_actual(
    ch_client: Client, 
    game_uid: UUID, 
    x: int, 
    y: int
):
    q = f"SELECT x, y, arrayElement(groupArray(value), indexOf(groupArray(epoch), max(epoch))) \
        FROM boards WHERE x=toUInt8(\'{x}\') and y=toUInt8(\'{y}\') and\
        game_uid=toUUID(\'{game_uid}\') group by (x, y)"
    
    res = ch_client.query_np(q)

    return res[0][2]
    

def get_max_epoch(
    ch_client: Client,
    game_uid: UUID
) -> int:
    q = f"select epoch from boards where \
        game_uid=toUUID(\'{game_uid}\') order by epoch desc limit 1"
    r = ch_client.query(q).first_row
    return r[0]


def get_cells_in_row(cells: list[tuple[int,int]]) -> list[tuple[int, int]]:
    walked = [cells[0]]
    for cell in cells[1:]:
        if abs(cell[0] - walked[-1][0]) > 1 or abs(cell[1] - walked[-1][1]) > 1:
            print(f"{walked=} break on {cell=}")
            break
        walked.append(cell)
    return walked


def check_swap_available_and_get_cells_to_fire(
    ch_client: Client,
    game_uid: UUID,
    x1: int,
    y1: int,
    x2: int,
    y2: int,
):
    q = "select x, y from boards where game_uid=toUUID(\'{game_uid}\') and (x=toUInt8(\'{x}\') \
        or y=toUInt8(\'{y}\')) and not (x=toUInt8(\'{x2}\') and y=toUInt8(\'{y2}\')) \
        and value=toInt8(\'{value}\')"
    current_epoch = get_max_epoch(ch_client=ch_client, game_uid=game_uid)

    print(current_epoch)

    v1 = get_cell_value_on_epoch(
        ch_client=ch_client,
        game_uid=game_uid,
        epoch=current_epoch,
        x=x1,
        y=y1
    )
    v2 = get_cell_value_on_epoch(
        ch_client=ch_client,
        game_uid=game_uid,
        epoch=current_epoch,
        x=x2,
        y=y2
    )

    print(f"{v1=} {v2=}")

    r1 = get_cells_in_row(sorted(ch_client.query(q.format_map({
        'game_uid': game_uid,
        'x': x1,
        'y': y1,
        'x2': x2,
        'y2': y2,
        'value': v2
    })).result_rows, key=lambda x: (x[0]-x2, x[1]-y2)))
    r2 = get_cells_in_row(sorted(ch_client.query(q.format_map({
        'game_uid': game_uid,
        'x': x2,
        'y': y2,
        'x2': x1,
        'y2': y1,
        'value': v1
    })).result_rows, key=lambda x: (x[0]-x1, x[1]-y1)))
    print(f"{r1=} {r2=}")
    r = []

    if len(r1) > 1:
        r1.append((x1, y1))
        print(r1)
        r = r + r1
    
    if len(r2) > 1:
        r2.append((x2, y2))
        r = r + r2

    return r


def swap_cells(
    ch_client: Client,
    game_uid: UUID,
    x1: int,
    y1: int,
    x2: int,
    y2: int,
):
    cells_to_fire = check_swap_available_and_get_cells_to_fire(ch_client, game_uid, x1, y1, x2, y2)

    if len(cells_to_fire) == 0:
        return False

    current_epoch = get_max_epoch(ch_client=ch_client, game_uid=game_uid)

    # insert 0 on cells_to_fire
    
    v1 = get_cell_value_on_epoch(
        ch_client=ch_client,
        game_uid=game_uid,
        epoch=current_epoch,
        x=x1,
        y=y1
    )
    v2 = get_cell_value_on_epoch(
        ch_client=ch_client,
        game_uid=game_uid,
        epoch=current_epoch,
        x=x2,
        y=y2
    )


    for_df = {
        'x': [x1, x2],
        'y': [y1, y2],
        'epoch': current_epoch + 1,
        'value': [v2, v1],
        'game_uid': game_uid
    }

    df = pd.DataFrame(for_df)

    ch_client.insert_df('boards', df)

    for_df = {
        'x': [],
        'y': [],
        'epoch': current_epoch + 2,
        'value': 0,
        'game_uid': game_uid
    }

    for cell in cells_to_fire:
        for_df['x'].append(cell[0])
        for_df['y'].append(cell[1])

    df = pd.DataFrame(for_df)
    print(f"{df.to_numpy()=}")
    ch_client.insert_df('boards', df)

    return True


def get_zero_cell_with_max_y(ch_client: Client, game_uid: UUID):
    q = f"select x, y from boards where game_uid=toUUID(\'{game_uid}\') \
        group by (x,y) \
        having arrayElement(groupArray(value), indexOf(groupArray(epoch), max(epoch))) = 0 \
        order by y asc \
        limit 1"
    
    res = ch_client.query_np(q)

    return res


def fill_zeros(ch_client: Client, game_uid: UUID):
    zero_cell = get_zero_cell_with_max_y(ch_client, game_uid)
    while len(zero_cell) > 0:
        act_cell = zero_cell[0]
        print(f"{act_cell=}")
        if act_cell[1] == 0:
            ch_client.insert_df('boards', pd.DataFrame({
                'x': [act_cell[0]],
                'y': [act_cell[1]],
                'value': [random.choice(values_available_for_generate)],
                'epoch': [get_max_epoch(ch_client, game_uid)+1],
                'game_uid': [game_uid]
            }))
        else:
            ch_client.insert_df('boards', pd.DataFrame({
                'x': [act_cell[0], act_cell[0]],
                'y': [act_cell[1], act_cell[1]-1],
                'value': [get_cell_value_actual(ch_client, game_uid, act_cell[0], act_cell[1]-1), 0],
                'epoch': get_max_epoch(ch_client, game_uid)+1,
                'game_uid': game_uid
            }))
        zero_cell = get_zero_cell_with_max_y(ch_client, game_uid)


def get_all_epochs_boards(ch_client: Client, game_uid: UUID):
    init_board = get_board_on_epoch(ch_client, game_uid, 0).tolist()
    boards = [init_board]
    for epoch in range(1, get_max_epoch(ch_client, game_uid)+1):
        board_on_epoch = get_board_on_epoch(ch_client, game_uid, epoch).tolist()
        on_epoch_coords = [(c[0], c[1]) for c in board_on_epoch]
        prev_board = deepcopy(boards[-1])
        curr_board = [c for c in prev_board if not (c[0], c[1]) in on_epoch_coords]
        curr_board = curr_board + board_on_epoch
        boards.append(list(curr_board))
    return boards


def trans_from_bd_to_python(board: list[list[int]]) -> list[list[int]]:
    res = []
    for cell in board:
        if len(res) < cell[1]+1:
            for _ in range(0, cell[1]-len(res)+1):
                res.append([])
        if len(res[cell[1]]) < cell[0]+1:
            for _ in range(0, cell[0]-len(res[cell[1]])+1):
                res[cell[1]].append(0)
        res[cell[1]][cell[0]] = cell[2]
    return res


def main():
    ch_client = get_client(
        host='localhost',
        username='default',
        database='default',
        port=18123
    )

    print(ch_client.command(crtb))
    game_uid = UUID('ead0f6fd-21e8-4897-97d7-9c742a01ee4a')#generate_board(ch_client)
    print(f"{game_uid=}")

    # sw_av = check_swap_available_and_get_cells_to_fire(ch_client, game_uid, 1, 2, 2, 2)

    # swap_cells(ch_client, game_uid, 1, 2, 2, 2)

    # print(get_board_on_epoch(ch_client, game_uid, get_max_epoch(ch_client, game_uid))-1)
    # print('--------')
    # print(get_board_on_epoch(ch_client, game_uid, get_max_epoch(ch_client, game_uid)))
    print('--------')
    print(get_actual_board(ch_client, game_uid))
    # fill_zeros(ch_client, game_uid)
    print(get_actual_board(ch_client, game_uid))
    # print(*get_all_epochs_boards(ch_client, game_uid), sep='\n~~~~~~~~~~~~~~~~~~~\n')
    boards = [trans_from_bd_to_python(b) for b in get_all_epochs_boards(ch_client, game_uid)]
    print('-------------')
    for board in boards:
        print(*board,sep='\n')
        print('~~~~~~~~~~~~~~~~')



if __name__ == '__main__':
    random.seed()
    main()

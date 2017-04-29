import arcpy
import math
import os
import pymysql.cursors
import re


class HexGrid:
    def __init__(self, size=100, orig_x=-20000000.0, orig_y=-20000000.0):
        self.orig_x = orig_x
        self.orig_y = orig_y
        self.size = size
        self.h = self.size * math.cos(30.0 * math.pi / 180.0)
        self.v = self.size * 0.5
        self.skip_x = 2.0 * self.h
        self.skip_y = 3.0 * self.v

    def rc2xy(self, r, c):
        ofs = self.h if r % 2 != 0 else 0
        x = c * self.skip_x + ofs + self.orig_x
        y = r * self.skip_y + self.orig_y
        return x, y


class HexCell:
    def __init__(self, size=100):
        self.xy = []
        for i in range(7):
            angle = math.pi * ((i % 6) + 0.5) / 3.0
            x = size * math.cos(angle)
            y = size * math.sin(angle)
            self.xy.append((x, y))

    def to_shape(self, cx, cy):
        return [[cx + x, cy + y] for (x, y) in self.xy]


class BaseTool(object):
    def __init__(self):
        self.RAD = 6378137.0
        self.RAD2 = self.RAD * 0.5
        self.LON = self.RAD * math.pi / 180.0
        self.D2R = math.pi / 180.0

    def lon2X(self, l):
        return l * self.LON

    def lat2Y(self, l):
        rad = l * self.D2R
        sin = math.sin(rad)
        return self.RAD2 * math.log((1.0 + sin) / (1.0 - sin))

    def delete_fc(self, fc):
        if arcpy.Exists(fc):
            arcpy.management.Delete(fc)

    def param_string(self, name="in_name", display_name="Label", value=""):
        param = arcpy.Parameter(
            name=name,
            displayName=display_name,
            direction="Input",
            datatype="String",
            parameterType="Required")
        param.value = value
        return param

    def param_host(self, display_name="Host", value="memsql.local"):
        return self.param_string(name="in_host", display_name=display_name, value=value)

    def param_size(self, display_name="Size in meters", value="100"):
        return self.param_string(name="in_size", display_name=display_name, value=value)

    def param_name(self, display_name="Layer name", value="output"):
        return self.param_string(name="in_name", display_name=display_name, value=value)

    def param_where(self, display_name="Where", value="passcount > 0"):
        return self.param_string(name="in_where", display_name=display_name, value=value)

    def param_fc(self):
        output_fc = arcpy.Parameter(
            name="output_fc",
            displayName="output_fc",
            direction="Output",
            datatype="Feature Layer",
            parameterType="Derived")
        return output_fc

    def isLicensed(self):
        return True

    def updateParameters(self, parameters):
        return

    def updateMessages(self, parameters):
        return


class Toolbox(object):
    def __init__(self):
        self.label = "MemSQL Toolbox"
        self.alias = "MemSQLToolbox"
        self.tools = [QueryTool, DensityTool, HexTool]


class QueryTool(BaseTool):
    def __init__(self):
        super(QueryTool, self).__init__()
        self.label = "Query Trips"
        self.description = "Tool to query trips table using MemSQL"
        self.canRunInBackground = True

    def getParameterInfo(self):
        return [
            self.param_fc(),
            self.param_name(value="trips"),
            self.param_where()
        ]

    def execute(self, parameters, messages):
        name = parameters[1].value

        in_memory = True
        if in_memory:
            ws = "in_memory"
            fc = ws + "/" + name
        else:
            fc = os.path.join(arcpy.env.scratchGDB, name)
            ws = os.path.dirname(fc)

        self.delete_fc(fc)

        sr_84 = arcpy.SpatialReference(4326)
        arcpy.management.CreateFeatureclass(ws, name, "POINT", spatial_reference=sr_84)
        arcpy.management.AddField(fc, "PICKUP_DT", "DATE")
        arcpy.management.AddField(fc, "PASS_COUNT", "SHORT")
        arcpy.management.AddField(fc, "TRIP_TIME", "SHORT")
        arcpy.management.AddField(fc, "TRIP_DIST", "FLOAT")

        if hasattr(arcpy, "mapping"):
            map_doc = arcpy.mapping.MapDocument('CURRENT')
            df = arcpy.mapping.ListDataFrames(map_doc)[0]
            extent_84 = df.extent.projectAs(sr_84)
        else:
            gis_project = arcpy.mp.ArcGISProject('CURRENT')
            map_frame = gis_project.listMaps()[0]
            extent_84 = map_frame.defaultCamera.getExtent().projectAs(sr_84)

        sql = """select
            ploc,
            pdate,
            passcount,
            triptime,
            tripdist
            from trips
            where {w}
            and GEOGRAPHY_CONTAINS("POLYGON(({xmin} {ymin},
            {xmax} {ymin},
            {xmax} {ymax},
            {xmin} {ymax},
            {xmin} {ymin}))",ploc)
            """.format(
            w=parameters[0].value,
            xmin=extent_84.XMin,
            ymin=extent_84.YMin,
            xmax=extent_84.XMax,
            ymax=extent_84.YMax
        )
        sql = re.sub(r'\s+', ' ', sql)
        host = os.getenv('MEMSQL_HOST', 'quickstart')
        connection = pymysql.connect(host=host,
                                     user='root',
                                     db='trips',
                                     charset='utf8mb4')
        try:
            with connection.cursor() as cursor:
                cursor.execute(sql)
                rows = cursor.fetchall()
                max_range = len(rows)
                cur_range = 0
                inc_range = max(1, int(max_range / 100))
                arcpy.SetProgressor("step", "Creating Features...", 0, max_range, inc_range)
                fields = ['SHAPE@WKT', 'PICKUP_DT', 'PASS_COUNT', 'TRIP_TIME', 'TRIP_DIST']
                with arcpy.da.InsertCursor(fc, fields) as cursor:
                    for row in rows:
                        cursor.insertRow(row)
                        arcpy.SetProgressorPosition(cur_range)
                        cur_range += 1
        finally:
            connection.close()
        arcpy.ResetProgressor()
        parameters[0].value = fc


class DensityTool(BaseTool):
    def __init__(self):
        super(DensityTool, self).__init__()
        self.label = "Trip Density"
        self.description = "Calculate density of trips"
        self.canRunInBackground = True

    def getParameterInfo(self):
        param_fc = self.param_fc()
        param_fc.symbology = os.path.join(os.path.dirname(__file__), "density.lyrx")
        return [self.param_size(), self.param_where(), self.param_name(value="density"), param_fc]

    def execute(self, parameters, messages):
        cell1 = float(parameters[0].value)
        cell2 = cell1 * 0.5
        where = parameters[1].value
        name = parameters[2].value

        in_memory = True
        if in_memory:
            ws = "in_memory"
            fc = ws + "/" + name
        else:
            fc = os.path.join(arcpy.env.scratchGDB, name)
            ws = os.path.dirname(fc)
        self.delete_fc(fc)

        sp_ref = arcpy.SpatialReference(102100)
        arcpy.management.CreateFeatureclass(ws, name, "POINT", spatial_reference=sp_ref)
        arcpy.management.AddField(fc, "POPULATION", "LONG")
        sql = """select
            T.C*{c1}+{c2} as X,
            T.R*{c1}+{c2} as Y,
            count(*) AS POPULATION from (
                select
                cast(floor(px/{c1}) as signed integer) as C,
                cast(floor(py/{c1}) as signed integer) as R
                from trips where {w}) T
                group by T.R,T.C
        """.format(w=where, c1=cell1, c2=cell2)
        sql = re.sub(r'\s+', ' ', sql)
        host = os.getenv('MEMSQL_HOST', 'quickstart')
        connection = pymysql.connect(host=host,
                                     user='root',
                                     db='trips',
                                     charset='utf8mb4')
        try:
            with connection.cursor() as cursor:
                cursor.execute(sql)
                rows = cursor.fetchall()
                max_range = len(rows)
                cur_range = 0
                inc_range = max(1, int(max_range / 100))
                arcpy.SetProgressor("step", "Creating Features...", 0, max_range, inc_range)
                with arcpy.da.InsertCursor(fc, ['SHAPE@X', 'SHAPE@Y', 'POPULATION']) as cursor:
                    for row in rows:
                        # cursor.insertRow([float(row[0]), float(row[1]), int(row[2])])
                        cursor.insertRow(row)
                        cur_range += 1
                        arcpy.SetProgressorPosition(cur_range)
                arcpy.ResetProgressor()
        finally:
            connection.close()

        parameters[3].value = fc


class HexTool(BaseTool):
    def __init__(self):
        super(HexTool, self).__init__()
        self.label = "Hex Density"
        self.description = "Calculate density based on hex cells"
        self.canRunInBackground = True

    def getParameterInfo(self):
        param_name = self.param_name(value="hex100")
        param_fc = self.param_fc()
        param_fc.symbology = os.path.join(os.path.dirname(__file__), "hex.lyrx")
        min_pop = arcpy.Parameter(
            name="in_min_pop",
            displayName="Min Pop Count",
            direction="Input",
            datatype="Long",
            parameterType="Required")
        min_pop.value = 10

        return [self.param_size(), self.param_where(), min_pop, param_name, param_fc]

    def execute(self, parameters, messages):
        cell = parameters[0].value
        size = float(cell)
        hex_cell = HexCell(size=size)
        hex_grid = HexGrid(size=size)
        where = parameters[1].value
        min_pop = parameters[2].value
        name = parameters[3].value

        sr_84 = arcpy.SpatialReference(4326)
        if hasattr(arcpy, "mapping"):
            map_doc = arcpy.mapping.MapDocument('CURRENT')
            df = arcpy.mapping.ListDataFrames(map_doc)[0]
            extent_84 = df.extent.projectAs(sr_84)
        else:
            gis_project = arcpy.mp.ArcGISProject('CURRENT')
            map_frame = gis_project.listMaps()[0]
            extent_84 = map_frame.defaultCamera.getExtent().projectAs(sr_84)

        in_memory = True
        if in_memory:
            ws = "in_memory"
            fc = ws + "/" + name
        else:
            fc = os.path.join(arcpy.env.scratchGDB, name)
            ws = os.path.dirname(fc)

        self.delete_fc(fc)

        sr_wm = arcpy.SpatialReference(102100)
        arcpy.management.CreateFeatureclass(ws, name, "POLYGON", spatial_reference=sr_wm)
        arcpy.management.AddField(fc, "POPULATION", "LONG")

        sql = """select p{c} as rc,count(p{c}) as pop
        from trips
        where {w}
        and GEOGRAPHY_CONTAINS("POLYGON(({xmin} {ymin},
        {xmax} {ymin},
        {xmax} {ymax},
        {xmin} {ymax},
        {xmin} {ymin}))",ploc)
        group by p{c}
        having pop > {p}
        """.format(c=cell, w=where, p=min_pop,
                   xmin=extent_84.XMin,
                   ymin=extent_84.YMin,
                   xmax=extent_84.XMax,
                   ymax=extent_84.YMax
                   )
        sql = re.sub(r'\s+', ' ', sql)
        host = os.getenv('MEMSQL_HOST', 'quickstart')
        connection = pymysql.connect(host=host,
                                     user='root',
                                     db='trips',
                                     charset='utf8mb4')
        try:
            with connection.cursor() as cursor:
                cursor.execute(sql)
                rows = cursor.fetchall()
                max_range = len(rows)
                cur_range = 0
                inc_range = max(1, int(max_range / 100))
                arcpy.SetProgressor("step", "Creating Features...", 0, max_range, inc_range)
                with arcpy.da.InsertCursor(fc, ['SHAPE@', 'POPULATION']) as cursor:
                    for row in rows:
                        r, c = row[0].split(":")
                        x, y = hex_grid.rc2xy(float(r), float(c))
                        cursor.insertRow([hex_cell.to_shape(x, y), row[1]])
                        arcpy.SetProgressorPosition(cur_range)
                        cur_range += 1
        finally:
            connection.close()

        arcpy.ResetProgressor()
        parameters[4].value = fc

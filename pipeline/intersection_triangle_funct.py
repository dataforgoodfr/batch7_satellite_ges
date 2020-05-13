# -*- coding: utf-8 -*-
"""
Created on Tue May 12 22:25:52 2020

@author: Christian Maréchal
"""

import os

if False:
    os.chdir('D:/ecomdata/DataForGood_Satelitte_CO2/batch7_satellite_ges')
    print(os.getcwd())

import math
import geopy
import geopy.distance

import folium
from folium import plugins

'''
Si V , u et v représentent les vitesses respectives du vent,
du vent zonal et du vent méridien en M,
la valeur de V est égale à la racine carrée du nombre u 2 + v 2 ;
quant à la direction du vent, qui désigne toujours la direction d'où vient le vent,
 elle peut par exemple être spécifiée d'après
 l'angle α que fait l'axe horizontal sud-nord passant par M
avec le support de la flèche du vent orienté dans le sens opposé à celle-ci
Direction = 180/pi * atan2(u,v)+180

'''
def get_direction_from_uv(u, v):
    ''' calculate direction from u and v components '''
    # Direction = 180/pi * atan2(u,v)+180
    direction = 180/math.pi * math.atan2(u,v)+180
    return direction


def draw_map(points, c=None, first_color='red', second_color='blue'):
    pts = points[0]
    coord = [pts[0], pts[1]]

    if c is None:
        c = folium.Map(location=coord, zoom_start=10)

    folium.PolyLine(points[0:2], color=first_color).add_to(c)
    folium.PolyLine(points[1:], color=second_color).add_to(c)

    c.save('A_Plugins_osm1.html')


def triangle_from_peak(pts=None, heading_wind=None, angle=45, distance=30):
    ''' create an opposite wind triangle from a peak '''

    '''
    pts : peak
    wind_heading : wind direction from 0 to 359
    angle : to create a cone
    at distance = 10km
    '''

    if pts is None:
        print('pts is none')
        return

    if heading_wind is None:
        print('heading_wind is none')
        return

    heading_opposit = (heading_wind+180)%360
    heading_opposit_a = (heading_opposit-angle)%360
    heading_opposit_b = (heading_opposit+angle)%360

    # Define starting point.
    start = geopy.Point(pts[0], pts[1])

    # Define a general distance object, initialized with a distance of 1 km.
    d = geopy.distance.VincentyDistance(kilometers = distance)
    desta = d.destination(point=start, bearing=heading_opposit_a)
    destb = d.destination(point=start, bearing=heading_opposit_b)
    triangle = [pts, [desta[0], desta[1]], [destb[0], destb[1]]]

    return triangle


def is_source_intrangle(src_pts, triangle, test=False):
    ''' check if a point is in a peak triangle '''

    x1 = triangle[0][0]
    y1 = triangle[0][1]
    x2 = triangle[1][0]
    y2 = triangle[1][1]
    x3 = triangle[2][0]
    y3 = triangle[2][1]
    xp = src_pts[0]
    yp = src_pts[1]
    c1 = (x2-x1)*(yp-y1)-(y2-y1)*(xp-x1)
    c2 = (x3-x2)*(yp-y2)-(y3-y2)*(xp-x2)
    c3 = (x1-x3)*(yp-y3)-(y1-y3)*(xp-x3)
    yes_inside = False
    if (c1<0 and c2<0 and c3<0) or (c1>0 and c2>0 and c3>0):
        yes_inside = True

    if test:
        if yes_inside:
            print("The point is in the triangle.")
        else:
            print("The point is outside the triangle.")

    return yes_inside

def test_triangle_from_peak():
    '''  basic test function '''
    pts = [43.819825, 1.371238]
    heading_wind = 0 # direction North, we loook for a point in south

    triangle = triangle_from_peak(pts, heading_wind, 45, 30)
    src_pts = [43.631208, 1.445351]
    is_source_intrangle(src_pts, triangle, True)
    src_pts = [43.592544, 1.500436]
    is_source_intrangle(src_pts, triangle, True)
    print(triangle)
    points = triangle + [pts]
    draw_map(points)

if False:
    test_triangle_from_peak()

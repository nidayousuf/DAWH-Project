import matplotlib; matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch
W,H=1471,826
fig=plt.figure(figsize=(W/100,H/100),dpi=100); ax=fig.add_axes([0,0,1,1]); ax.set_xlim(0,W); ax.set_ylim(H,0); ax.axis('off')
F='DejaVu Sans'; LH=18.7
def box(x,y,w,title,rows,color):
    h=34+len(rows)*LH+14
    ax.add_patch(FancyBboxPatch((x,y),w,h,boxstyle="round,pad=0,rounding_size=6",fc='white',ec='#333',lw=1.5))
    ax.add_patch(FancyBboxPatch((x,y),w,30,boxstyle="round,pad=0,rounding_size=6",fc=color,ec='#333',lw=1.5))
    ax.text(x+w/2,y+15,title,ha='center',va='center',fontsize=10.5,fontweight='bold',family=F)
    for i,r in enumerate(rows):
        pre,name=(r.split(' ',1) if r[:3] in('PK ','FK ') else ('',r))
        yy=y+46+i*LH
        if pre: ax.text(x+12,yy,pre,fontsize=8,va='center',family=F)
        ax.text(x+31,yy,name,fontsize=8,va='center',family=F)
    return (x,y,w,h)
def line(pts,label=None,lpos=None,one=None,many=None):
    xs,ys=zip(*pts); ax.plot(xs,ys,color='#333',lw=1.5)
    if label: ax.text(*lpos,label,fontsize=8,style='italic',family=F,ha='center',va='center',bbox=dict(fc='white',ec='none',pad=1))
    if one: ax.text(*one,'1',color='#c00000',fontsize=10,fontweight='bold',ha='center',va='center')
    if many: ax.text(*many,'*',color='#c00000',fontsize=13,fontweight='bold',ha='center',va='center')
ax.text(W/2,22,'Fact Constellation: Sales & Demand Intelligence (cold-chain pharma)',ha='center',va='center',fontsize=13,fontweight='bold',family=F)
hosp=box(22,157,226,'DimHospital',['PK Hosp.Key','Hosp.ID','Hosp.GLN','Hosp.Name','Hosp.Type','Trauma.Level'],'#c9b3e6')
fd=box(314,157,238,'FactDemand',['PK Demand.Key','FK Date.Key','FK Med.Key','FK Hosp.Key','Qty.Forecast','Qty.Requested','Qty.Approved','Qty.Received','Qty.Consumed','Qty.Wasted','Qty.Unfulfilled','Qty.ClosingStock','Stockout.Days','Lead.Time.Days','Unfulfilled.Value.USD','Licensed.Beds','ICU.Beds','Occupancy.Pct','Catchment.Pop','Est.Disease.Cases','Disease.Admissions','Outpatient.Encounters'],'#f4b183')
dd=box(627,67,216,'DimDate (role-playing)',['PK Date.Key','Month.StartDate','Month.No','Month.Name','Quarter','Year','Days.InMonth','Full.Date'],'#9dc3e6')
dm=box(627,387,216,'DimMedicine',['PK Med.Key','Med.ID','InternalSKU','GTIN14','Med.Name','ICD10.Code','Disease.Cat','Med.Class','Dosage.Strength','Admin.Route','Pack.Size','Presc.Class','Legal.Controlled','Cold.Chain','Shelf.LifeMonths'],'#a9d18e')
fs=box(919,157,238,'FactSales',['PK Sales.Key','FK Date.Key','FK Med.Key','FK Location.Key','FK Hosp.Key','FK Pharm.Key','Sales.Txn.ID','Sale.Status','Return.Flag','Sales.Channel','Qty.Sold','Qty.Returned','Sales.Tax.USD','Gross.Sales.USD','Discount.USD','Net.Sales.USD','COGS.USD','Gross.Margin.USD','Days.To.Expiry'],'#f4b183')
dl=box(1222,67,227,'DimLocation',['PK Location.Key','City','State','Region','Country'],'#ffd966')
dp=box(1222,330,227,'DimPharmacy',['PK Pharm.Key','Pharm.ID','Pharm.GLN','Pharm.Name','Pharm.Type'],'#f8cbad')
# links
line([(248,223),(314,223)],'Hosp.Key',(281,209),None,(308,208))
line([(552,222),(627,160)],'Date.Key',(590,176),(620,153),(565,201))
line([(552,388),(627,420)],'Med.Key',(590,390),(620,405),(565,378))
line([(843,148),(919,222)],'Date.Key',(881,176),(855,154),(913,201))
line([(843,420),(919,388)],'Med.Key',(881,390),(855,405),(913,378))
line([(1157,222),(1222,140)],'Location.Key',(1190,168),(1216,140),(1170,200))
line([(1157,305),(1222,395)],'Pharm.Key',(1190,345),(1216,385),(1170,318))
line([(130,hosp[1]+hosp[3]),(130,766),(1038,766),(1038,fs[1]+fs[3])],'Hosp.Key',(584,752),(143,336),(1052,575))
ax.text(W-22,803,'FK = foreign key.  1 : * = one dimension row to many fact rows.  DimDate, DimMedicine and DimHospital are shared (conformed) by both facts.',ha='right',fontsize=7.5,color='#555',family=F)
import sys; plt.savefig(sys.argv[1] if len(sys.argv)>1 else 'docs/sdi_star_schema.png',dpi=100); print('ok')
